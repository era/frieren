use crate::backend::types::{
    expr_predicates, object_name_to_namespace, object_name_to_table, select_item, table_factor,
    type_for,
};
use crate::error::Error;
use crate::frontend::Query;
use arrow::record_batch::RecordBatch;
use futures_util::{StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Snapshot};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{io::FileIOBuilder, Catalog};
use iceberg::{Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use sqlparser::ast::{CreateTable, DataType, ObjectName, ObjectType, Use};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

mod types;

#[derive(Debug, PartialEq)]
pub enum Output {
    CreatedDatabase(String),
    Use(Context),
    CreatedTable(String),
    Drop(Vec<String>),
    Select(Vec<RecordBatch>),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Context {
    namespace: Option<Namespace>,
}

/// Storage (wrapper around Apache Iceberg)
///
/// Note:
/// we don't support any isolation between queries,
/// which means we have dirty reads all types of nonsense for now.
pub struct Storage {
    catalog: Arc<RestCatalog>, //TODO:<dyn Catalog>,
}

impl Storage {
    pub fn new() -> Result<Self, Error> {
        //let catalog = Arc::new(new_memory_catalog()?);
        let catalog = Arc::new(new_rest_catalog()?);
        Ok(Self { catalog })
    }
    pub async fn execute(&self, sql: Query) -> Result<Vec<Output>, Error> {
        let mut context = Context::default();
        let mut results = Vec::with_capacity(sql.stmts.len());
        for stmt in sql.stmts {
            let result = self.execute_stmt(context.clone(), stmt).await?;
            // if the previous statement was a `use` we should start using that namespace
            if let Output::Use(ctx) = result {
                context = ctx;
            } else {
                // otherwise we should push the results to be retrieved by the user later
                results.push(result);
            }
        }
        Ok(results)
    }
    async fn execute_stmt(
        &self,
        ctx: Context,
        stmt: sqlparser::ast::Statement,
    ) -> Result<Output, Error> {
        match stmt {
            sqlparser::ast::Statement::Query(query) => {
                Ok(Output::Select(self.select(ctx, query).await?))
            }
            sqlparser::ast::Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(Output::CreatedDatabase(
                self.create_database(ctx, db_name, if_not_exists)
                    .await?
                    .name()
                    .to_url_string(),
            )),
            sqlparser::ast::Statement::Use(u) => Ok(Output::Use(self.use_stmt(ctx, u).await?)),
            sqlparser::ast::Statement::CreateTable(create_table) => Ok(Output::CreatedTable(
                self.create_table(ctx, create_table)
                    .await?
                    .identifier()
                    .name
                    .clone(),
            )),
            sqlparser::ast::Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => Ok(Output::Drop(
                self.drop_object(ctx, object_type, if_exists, names).await?,
            )),
            e => Err(Error::NotSupportedSql(e.to_string())),
        }
    }

    async fn select(
        &self,
        ctx: Context,
        query: Box<sqlparser::ast::Query>,
    ) -> Result<Vec<RecordBatch>, Error> {
        match *query.body {
            sqlparser::ast::SetExpr::Select(select) => {
                //FIXME: not handling joins
                let table_name =
                    table_factor(ctx.clone(), select.from.get(0).cloned().unwrap().relation)?;
                let projection: Vec<String> =
                    select.projection.into_iter().map(select_item).collect();
                let predicate = expr_predicates(select.selection);
                self.scan(table_name, projection, predicate).await
            }
            _ => unimplemented!(),
        }
    }

    async fn drop_object(
        &self,
        ctx: Context,
        object_type: ObjectType,
        if_exists: bool,
        names: Vec<ObjectName>,
    ) -> Result<Vec<String>, Error> {
        let mut dropped_items = Vec::new();
        match object_type {
            ObjectType::Table => {
                for name in names {
                    let table = object_name_to_table(ctx.clone(), name)?;
                    if if_exists && !self.catalog.table_exists(&table).await? {
                        return Err(Error::NotPossibletoDrop(table.name));
                    }
                    self.catalog.drop_table(&table).await?;
                    dropped_items.push(table.name);
                }
            }
            ObjectType::Database | ObjectType::Schema => {
                for name in names {
                    let namespace = object_name_to_namespace(ctx.clone(), name)?;

                    if if_exists && !self.catalog.namespace_exists(&namespace).await? {
                        return Err(Error::NotPossibletoDrop(namespace.to_url_string()));
                    }

                    self.catalog.drop_namespace(&namespace).await?;
                    dropped_items.push(namespace.to_url_string());
                }
            }
            _ => return Err(Error::NotSupportedSql("Cannot drop the item".to_string())),
        };
        Ok(dropped_items)
    }

    async fn use_stmt(&self, ctx: Context, u: Use) -> Result<Context, Error> {
        match u {
            // for us all means the same
            Use::Schema(name) | Use::Database(name) | Use::Object(name) => {
                // if the name exists
                let namespace_ident = object_name_to_namespace(ctx, name)?;
                let namespace = self.catalog.get_namespace(&namespace_ident).await?;
                Ok(Context {
                    namespace: Some(namespace),
                })
            }
            _ => Err(Error::NotSupportedSql(
                "only use schema and use database supported".to_string(),
            )),
        }
    }

    async fn create_database(
        &self,
        ctx: Context,
        db_name: sqlparser::ast::ObjectName,
        if_not_exists: bool,
    ) -> Result<Namespace, Error> {
        // we use namespace as database, so if the user created a `my_cool_schema.my_cool_db`
        let namespace_ident = object_name_to_namespace(ctx, db_name)?;
        //check if exists and what if_not_exists is set to do
        match self.catalog.namespace_exists(&namespace_ident).await? {
            true if !if_not_exists => {
                Err(Error::DatabaseAlreadyExist(namespace_ident.to_url_string()))
            }
            false => Ok(self
                .catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await?),
            true => Ok(self.catalog.get_namespace(&namespace_ident).await?), // if already exists, but `if_not_exists` just keep going
        }
    }

    async fn create_table(&self, ctx: Context, table: CreateTable) -> Result<Table, Error> {
        let table_identifier = object_name_to_table(ctx, table.name)?;

        let (fields, mut err): (Vec<Result<NestedField, Error>>, _) = table
            .columns
            .iter()
            .enumerate()
            .map(|(id, value)| {
                Ok::<NestedField, Error>(NestedField::new(
                    id as i32,
                    value.name.value.clone(),
                    type_for(&value.data_type)?.into(),
                    false, // assuming all types are not required
                ))
            })
            .partition(Result::is_ok);

        if err.len() > 0 {
            // Kinda ugly, but if any error, just return the first
            return Err(err.pop().unwrap().unwrap_err());
        }

        let schema = Schema::builder()
            .with_fields(fields.into_iter().map(|f| Arc::new(f.unwrap().into())))
            .build()?;
        let creation = TableCreation::builder()
            // .location(
            //     self.path
            //         .join(&table_identifier.name)
            //         .to_str()
            //         .unwrap()
            //         .to_owned(),
            // )
            .name(table_identifier.name)
            .schema(schema)
            .build();

        Ok(self
            .catalog
            .create_table(&table_identifier.namespace, creation)
            .await?)
    }

    /// Write an Arrow RecordBatch into Table (writes to a parquet file and updates iceberg metadata).
    async fn write(&self, name: TableIdent, batch: RecordBatch) -> Result<(), Error> {
        // iceberg transaction
        let table = self.catalog.load_table(&name).await?;
        let transaction = Transaction::new(&table);
        let mut fast_append = transaction.fast_append(None, vec![])?;

        // metadata for parquet writer
        let file_io = table.file_io();
        let location = DefaultLocationGenerator::new(table.metadata().clone())?;

        // writing files to parquet first
        let prefix = format!("{}-{}", name.name, Uuid::new_v4());
        let file_name_generator = DefaultFileNameGenerator::new(
            prefix,
            None,                                   //suffix
            iceberg::spec::DataFileFormat::Parquet, //format
        );
        let parquet_props = parquet::file::properties::WriterProperties::builder().build();
        let parquet_writer_builder = ParquetWriterBuilder::new(
            parquet_props,
            table.metadata().current_schema().clone(),
            file_io.clone(),
            location,
            file_name_generator,
        );

        // just one partition `0` for now
        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
        let mut writer = data_file_writer_builder.build().await?;

        writer.write(batch).await?;

        let data_files = writer.close().await?;

        // now write to the metadata of our table
        fast_append.add_data_files(data_files)?;
        let transaction = fast_append.apply().await?;

        transaction.commit(&*self.catalog).await?;

        Ok(())
    }

    /// Pushes down predicates and projection to iceberg to handle
    /// any aggregation must be done by caller.
    async fn scan(
        &self,
        table_name: TableIdent,
        columns: impl IntoIterator<Item = String>,
        predicate: Predicate,
    ) -> Result<Vec<RecordBatch>, Error> {
        let table = self.catalog.load_table(&table_name).await?;
        let rows = table
            .scan()
            .select(columns)
            .with_filter(predicate)
            .build()?
            .to_arrow()
            .await?
            .try_collect()
            .await?;
        Ok(rows)
    }
}

fn new_memory_catalog() -> Result<MemoryCatalog, Error> {
    let file_io = FileIOBuilder::new_fs_io().build()?;
    Ok(MemoryCatalog::new(file_io, None))
}

fn new_rest_catalog() -> Result<RestCatalog, Error> {
    //FIXME: hardcoded S3 config
    let mut properties = HashMap::new();
    properties.insert(S3_REGION.to_string(), "us-east-1".to_string());
    properties.insert(S3_ACCESS_KEY_ID.to_string(), "admin".to_string());
    properties.insert(S3_SECRET_ACCESS_KEY.to_string(), "password".to_string());
    properties.insert(
        S3_ENDPOINT.to_string(),
        "http://localhost:9000/".to_string(),
    );
    // FIXME: hardcoded url
    Ok(RestCatalog::new(
        RestCatalogConfig::builder()
            .props(properties)
            .uri("http://0.0.0.0:8181".to_string())
            .build(),
    ))
}
#[derive(Debug)]
struct WalRecover {
    catalog: Arc<dyn Catalog>,
}

#[cfg(test)]
mod tests {
    use crate::frontend::parse;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::Field;
    use std::path::Path;

    use super::*;

    #[ignore]
    #[tokio::test]
    #[serial_test::serial]
    pub async fn create_database() {
        let storage = Storage::new().unwrap();
        // in case it existed
        let _ = storage
            .execute(parse("drop database a_database;").unwrap())
            .await;
        let sql = parse("create database a_database;").unwrap();
        assert_eq!(
            vec![Output::CreatedDatabase("a_database".to_string())],
            storage.execute(sql).await.unwrap()
        );

        let sql = parse("create database if not exists a_database;").unwrap();
        assert_eq!(
            vec![Output::CreatedDatabase("a_database".to_string())],
            storage.execute(sql).await.unwrap()
        );
        let sql = parse("create database a_database;").unwrap();
        assert!(storage.execute(sql).await.is_err());

        let sql = parse("create database a_database.inside_another;").unwrap();
        assert_eq!(
            vec![Output::CreatedDatabase(
                "a_database\u{1f}inside_another".to_string()
            )],
            storage.execute(sql).await.unwrap()
        );

        storage
            .execute(parse("use a_database; drop database inside_another;").unwrap())
            .await
            .unwrap();

        storage
            .execute(parse("drop database a_database;").unwrap())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial_test::serial]
    pub async fn create_drop() {
        let storage = Storage::new().unwrap();
        let sql = parse(
            "create database if not exists a_database; use a_database; create table my_table (a int32, b string); drop table my_table;",
        )
        .unwrap();
        assert_eq!(
            vec![
                Output::CreatedDatabase("a_database".to_string()),
                Output::CreatedTable("my_table".to_string()),
                Output::Drop(vec!["my_table".to_string()]),
            ],
            storage.execute(sql).await.unwrap()
        );

        let _ = storage
            .execute(parse("drop database a_database;").unwrap())
            .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    pub async fn write_read() {
        let r = RecordBatch::try_new(
            arrow::datatypes::Schema::new(vec![
                Field::new("id", arrow::datatypes::DataType::Int32, false).with_metadata(
                    HashMap::from([
                        ("PARQUET:field_id".to_string(), "1".to_string()), // Iceberg uses this
                    ]),
                ),
                Field::new("name", arrow::datatypes::DataType::Utf8, false).with_metadata(
                    HashMap::from([
                        ("PARQUET:field_id".to_string(), "2".to_string()), // Iceberg uses this
                    ]),
                ),
            ])
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        )
        .unwrap();

        let storage = Storage::new().unwrap();
        let sql = parse(
            "create database if not exists a_database; use a_database; create table if not exists my_table (id int32, name string);"
        )
            .unwrap();
        storage.execute(sql).await.unwrap();
        let table = TableIdent::new(
            NamespaceIdent::new("a_database".to_string()),
            "my_table".to_string(),
        );
        storage.write(table.clone(), r).await.unwrap();

        let results = storage
            .scan(
                table.clone(),
                vec!["id".to_string(), "name".to_string()],
                Predicate::AlwaysTrue,
            )
            .await
            .unwrap();
        let result = results.get(0).unwrap();
        assert_eq!(result.column_by_name("id").unwrap().len(), 3);
        assert_eq!(result.column_by_name("name").unwrap().len(), 3);

        let select = parse("use a_database; select id from my_table;").unwrap();
        let result = storage.execute(select).await.unwrap();
        let result = result.get(0).unwrap();
        if let Output::Select(results) = result {
            let result = results.get(0).unwrap();
            assert_eq!(result.column_by_name("id").unwrap().len(), 3);
        } else {
            panic!("should return a select result");
        }
        storage
            .execute(parse("use a_database; drop table my_table;").unwrap())
            .await
            .unwrap();
        let _ = storage
            .execute(parse("drop database a_database;").unwrap())
            .await;
    }
}
