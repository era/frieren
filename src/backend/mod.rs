use crate::error::Error;
use crate::frontend::Query;
use arrow::record_batch::RecordBatch;
use futures_util::{StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::spec::{NestedField, PrimitiveType, Schema};
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
use sqlparser::ast::{CreateTable, DataType, ObjectName, ObjectType, Use};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum Output {
    CreatedDatabase(String),
    Use(Context),
    CreatedTable(String),
    Drop(Vec<String>),
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
    path: PathBuf,
    catalog: Arc<dyn Catalog>,
}

impl Storage {
    pub fn new(path: PathBuf) -> Result<Self, Error> {
        let catalog = Arc::new(new_memory_catalog()?);

        Ok(Self { path, catalog })
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
            .location(
                self.path
                    .join(&table_identifier.name)
                    .to_str()
                    .unwrap()
                    .to_owned(),
            )
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

        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None);
        let mut writer = data_file_writer_builder.build().await?;

        writer.write(batch).await?;

        let data_files = writer.close().await?;

        // now write to the metadata of our table
        fast_append.add_data_files(data_files)?;
        fast_append.apply().await?;

        Ok(())
    }

    async fn read(
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

fn type_for(sql_type: &DataType) -> Result<PrimitiveType, Error> {
    match sql_type {
        DataType::Boolean | DataType::Bool => Ok(PrimitiveType::Boolean),
        DataType::Int32 => Ok(PrimitiveType::Int),
        DataType::Int64 => Ok(PrimitiveType::Long),
        DataType::Date => Ok(PrimitiveType::Date),
        DataType::Float32 => Ok(PrimitiveType::Float),
        DataType::Float64 => Ok(PrimitiveType::Double),
        DataType::String(_) => Ok(PrimitiveType::String),
        _ => Err(Error::NotSupportedSql(
            "column type is not supported".to_string(),
        )),
    }
}

fn object_name_to_namespace(
    ctx: Context,
    db_name: sqlparser::ast::ObjectName,
) -> Result<NamespaceIdent, Error> {
    // we will create a namespace my_cool_schema with a subnamespace called `my_cool_db`.
    // FIXME: should look at context to see if the user already used something like `use my_cool_schema;`
    // FIXME: on previous statements
    Ok(NamespaceIdent::from_vec(
        db_name
            .0
            .into_iter()
            // it's safe to unwrap as_indent because as now there will always be one there
            .map(|i| i.as_ident().unwrap().value.clone())
            .collect(),
    )?)
}

fn object_name_to_table(
    ctx: Context,
    table_name: sqlparser::ast::ObjectName,
) -> Result<TableIdent, Error> {
    if table_name.0.len() > 1 {
        return Err(Error::NotSupportedSql(
            "Syntax schema.table not supported yet".to_string(),
        ));
    }

    let namespace = match ctx.namespace {
                None => return Err(Error::NotSupportedSql("Must first select a database by using `use database` or a schema by using `use schema`".to_string())),
                Some(namespace) => namespace,
        };

    Ok(TableIdent::new(
        namespace.name().clone(),
        table_name
            .0
            .into_iter()
            // it's safe to unwrap as_indent because as now there will always be one there
            .map(|i| i.as_ident().unwrap().value.clone())
            .collect(),
    ))
}

fn new_memory_catalog() -> Result<MemoryCatalog, Error> {
    let file_io = FileIOBuilder::new_fs_io().build()?;
    Ok(MemoryCatalog::new(file_io, None))
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

    use super::*;

    #[tokio::test]
    pub async fn create_database() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_owned()).unwrap();
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
    }

    #[tokio::test]
    pub async fn create_drop() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_owned()).unwrap();
        let sql = parse(
            "create database a_database; use a_database; create table my_table (a int32, b string); drop table my_table; drop database a_database;",
        )
        .unwrap();
        assert_eq!(
            vec![
                Output::CreatedDatabase("a_database".to_string()),
                Output::CreatedTable("my_table".to_string()),
                Output::Drop(vec!["my_table".to_string()]),
                Output::Drop(vec!["a_database".to_string()]),
            ],
            storage.execute(sql).await.unwrap()
        );
    }

    #[tokio::test]
    pub async fn write() {
        let r = RecordBatch::try_new(
            arrow::datatypes::Schema::new(vec![
                Field::new("id", arrow::datatypes::DataType::Int32, false),
                Field::new("name", arrow::datatypes::DataType::Utf8, false),
            ])
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path().to_owned()).unwrap();
        let sql = parse(
            "create database a_database; use a_database; create table my_table (id int32, name string);"
        )
            .unwrap();
        storage.execute(sql).await.unwrap();
        let table = TableIdent::new(
            NamespaceIdent::new("a_database".to_string()),
            "my_table".to_string(),
        );
        storage.write(table.clone(), r).await.unwrap();

        //TODO verify
    }
}
