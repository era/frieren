use crate::backend::types::{
    object_name_to_namespace, object_name_to_table, table_factor, type_for,
};
use crate::backend::write::{align_batch, build_insert_batch, parquet_writer_properties};
use crate::error::Error;
use crate::plan::Binder;
use arrow::record_batch::RecordBatch;
use futures_util::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::expr::Predicate;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{DataFile, NestedField, Schema};
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{io::FileIOBuilder, Catalog};
use iceberg::{Namespace, TableCreation, TableIdent};
use iceberg_catalog_memory::MemoryCatalog;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use sqlparser::ast::{CreateTable, ObjectName, ObjectType, Use};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

mod eval;
mod types;
mod write;

//FIXME: Currently does not properly supports different types of numbers
// while querying.

#[derive(Debug, PartialEq)]
pub enum Output {
    CreatedDatabase(String),
    Use(Context),
    CreatedTable(String),
    Drop(Vec<String>),
    Inserted { table: String, rows: usize },
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
    pub async fn execute(&self, sql: &str) -> Result<Vec<Output>, Error> {
        let stmts = crate::parse(sql)?;
        let mut context = Context::default();
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in stmts {
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
            sqlparser::ast::Statement::Insert(insert) => Ok(self.insert(ctx, insert).await?),
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
        let select = match query.body.as_ref() {
            sqlparser::ast::SetExpr::Select(select) => select,
            _ => {
                return Err(Error::NotSupportedSql(
                    "only a plain SELECT is supported".to_string(),
                ))
            }
        };
        if select.from.len() != 1 {
            return Err(Error::NotSupportedSql(
                "exactly one FROM table is supported".to_string(),
            ));
        }

        let table_name = table_factor(ctx, select.from[0].relation.clone())?;
        let table = self.catalog.load_table(&table_name).await?;

        // Pin the snapshot and schema once; binding and execution must agree.
        let metadata = table.metadata();
        let snapshot_id = metadata.current_snapshot_id();
        let schema = match snapshot_id {
            Some(snapshot_id) => metadata
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| Error::NotSupportedSql(format!("snapshot {snapshot_id} not found")))?
                .schema(metadata)?,
            None => metadata.current_schema().clone(),
        };

        let plan = Binder::new(table_name, &schema, snapshot_id).bind(&query)?;

        let columns: Vec<&str> = plan
            .scan
            .projection
            .iter()
            .map(|field_id| {
                schema.name_by_field_id(*field_id).ok_or_else(|| {
                    Error::Internal(format!(
                        "planned field id {field_id} is missing from the pinned schema"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        let batches = self
            .scan_table(
                &table,
                plan.scan.snapshot_id,
                columns,
                plan.scan.predicate.clone().unwrap_or(Predicate::AlwaysTrue),
            )
            .await?;

        // Iceberg handled the pushed-down predicate; apply the residual and
        // then reduce to the user's projection, in SQL order.
        let mut results = Vec::with_capacity(batches.len());
        for batch in batches {
            let batch = match &plan.scan.residual {
                Some(residual) => eval::filter_record_batch(&batch, residual)?,
                None => batch,
            };
            results.push(eval::project_output(&batch, &plan.output)?);
        }
        Ok(results)
    }

    async fn insert(&self, ctx: Context, insert: sqlparser::ast::Insert) -> Result<Output, Error> {
        let name = match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => name.clone(),
            _ => {
                return Err(Error::NotSupportedSql(
                    "only INSERT INTO table is supported".to_string(),
                ))
            }
        };
        let table_name = object_name_to_table(ctx, name)?;
        let table = self.catalog.load_table(&table_name).await?;
        let schema = table.metadata().current_schema().clone();

        let plan = Binder::new(
            table_name.clone(),
            &schema,
            table.metadata().current_snapshot_id(),
        )
        .bind_insert(&insert)?;
        let arrow_schema = schema_to_arrow_schema(&schema)?;
        let batch = build_insert_batch(&schema, &arrow_schema, &plan.columns, &plan.rows)?;
        let rows = batch.num_rows();

        self.write(plan.table, vec![batch]).await?;
        Ok(Output::Inserted {
            table: table_name.name,
            rows,
        })
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

    /// Writes Arrow batches into a table. One call is one Iceberg transaction.
    async fn write(&self, name: TableIdent, batches: Vec<RecordBatch>) -> Result<(), Error> {
        if batches.is_empty() {
            return Ok(());
        }

        let mut table = self.catalog.load_table(&name).await?;
        let spec = table.metadata().default_partition_spec();
        if !spec.is_unpartitioned() {
            return Err(Error::NotSupportedSql(
                "partitioned tables are not supported yet".to_string(),
            ));
        }

        // The table schema is the source of truth for Arrow field IDs and types.
        let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())?;
        let batches = batches
            .into_iter()
            .map(|batch| align_batch(&batch, &arrow_schema))
            .collect::<Result<Vec<_>, _>>()?;

        // Write the data files once; retries only re-commit the metadata.
        let data_files = self.write_data_files(&table, batches).await?;
        let max_retries = table
            .metadata()
            .properties()
            .get("commit.retry.num-retries")
            .and_then(|value| value.parse().ok())
            .unwrap_or(3u32);
        let mut attempt = 0u32;

        loop {
            if attempt > 0 {
                table = self.catalog.load_table(&name).await?;
            }

            let transaction = Transaction::new(&table);
            let mut append = transaction.fast_append(None, vec![])?;
            append.add_data_files(data_files.clone())?;
            let transaction = append.apply().await?;

            match transaction.commit(&*self.catalog).await {
                Ok(_) => return Ok(()),
                Err(error) if is_commit_conflict(&error) && attempt < max_retries => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt)))
                        .await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn write_data_files(
        &self,
        table: &Table,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<DataFile>, Error> {
        let spec = table.metadata().default_partition_spec();
        let file_io = table.file_io();
        let location = DefaultLocationGenerator::new(table.metadata().clone())?;

        let prefix = format!("{}-{}", table.identifier().name(), Uuid::new_v4());
        let file_name_generator =
            DefaultFileNameGenerator::new(prefix, None, iceberg::spec::DataFileFormat::Parquet);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            parquet_writer_properties(table.metadata()),
            table.metadata().current_schema().clone(),
            file_io.clone(),
            location,
            file_name_generator,
        );

        let mut writer = DataFileWriterBuilder::new(parquet_writer_builder, None, spec.spec_id())
            .build()
            .await?;

        for batch in batches {
            writer.write(batch).await?;
        }

        Ok(writer.close().await?)
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
        self.scan_table(&table, None, columns, predicate).await
    }

    async fn scan_table(
        &self,
        table: &Table,
        snapshot_id: Option<i64>,
        columns: impl IntoIterator<Item = impl ToString>,
        predicate: Predicate,
    ) -> Result<Vec<RecordBatch>, Error> {
        let mut scan = table.scan();
        if let Some(snapshot_id) = snapshot_id {
            scan = scan.snapshot_id(snapshot_id);
        }
        let rows = scan
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
fn is_commit_conflict(error: &iceberg::Error) -> bool {
    error.message().contains("CommitFailedException")
}

#[derive(Debug)]
struct WalRecover {
    catalog: Arc<dyn Catalog>,
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::Field;
    use iceberg::NamespaceIdent;

    use super::*;

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            arrow::datatypes::Schema::new(vec![
                Field::new("id", arrow::datatypes::DataType::Int32, false),
                Field::new("name", arrow::datatypes::DataType::Utf8, false),
            ])
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[ignore]
    #[tokio::test]
    #[serial_test::serial]
    pub async fn create_database() {
        let storage = Storage::new().unwrap();
        // in case it existed
        let _ = storage.execute("drop database a_database;").await;
        let sql = "create database a_database;";
        assert_eq!(
            vec![Output::CreatedDatabase("a_database".to_string())],
            storage.execute(sql).await.unwrap()
        );

        let sql = "create database if not exists a_database;";
        assert_eq!(
            vec![Output::CreatedDatabase("a_database".to_string())],
            storage.execute(sql).await.unwrap()
        );
        let sql = "create database a_database;";
        assert!(storage.execute(sql).await.is_err());

        let sql = "create database a_database.inside_another;";
        assert_eq!(
            vec![Output::CreatedDatabase(
                "a_database\u{1f}inside_another".to_string()
            )],
            storage.execute(sql).await.unwrap()
        );

        storage
            .execute("use a_database; drop database inside_another;")
            .await
            .unwrap();

        storage.execute("drop database a_database;").await.unwrap();
    }

    #[tokio::test]
    #[serial_test::serial]
    pub async fn create_drop() {
        let storage = Storage::new().unwrap();
        let sql = "create database if not exists a_database; use a_database; create table my_table (a int32, b string); drop table my_table;";
        assert_eq!(
            vec![
                Output::CreatedDatabase("a_database".to_string()),
                Output::CreatedTable("my_table".to_string()),
                Output::Drop(vec!["my_table".to_string()]),
            ],
            storage.execute(sql).await.unwrap()
        );

        let _ = storage.execute("drop database a_database;").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    pub async fn write_read() {
        let r = test_batch();

        let storage = Storage::new().unwrap();
        let sql = "create database if not exists a_database; use a_database; create table if not exists my_table (id int32, name string);";
        storage.execute(sql).await.unwrap();
        let table = TableIdent::new(
            NamespaceIdent::new("a_database".to_string()),
            "my_table".to_string(),
        );
        storage.write(table.clone(), vec![r]).await.unwrap();

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

        let select = "use a_database; select id, name from my_table;";
        let result = storage.execute(select).await.unwrap();
        let result = match result.get(0).unwrap() {
            Output::Select(results) => results.get(0).unwrap(),
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "name");
        assert_eq!(result.num_rows(), 3);

        // The filter column is pushed down and is not part of the output or the scan projection.
        let select = "use a_database; select name from my_table where id = 4;";
        let result = storage.execute(select).await.unwrap();
        let result = match result.get(0).unwrap() {
            Output::Select(results) => results.get(0).unwrap(),
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(result.num_columns(), 1);
        assert!(result.column_by_name("id").is_none());
        let names = result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names.value(0), "a");

        // A residual filter keeps its input column for filtering but not for output.
        let select = "use a_database; select name from my_table where id = id;";
        let result = storage.execute(select).await.unwrap();
        let result = match result.get(0).unwrap() {
            Output::Select(results) => results.get(0).unwrap(),
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(result.num_columns(), 1);
        assert!(result.column_by_name("id").is_none());
        assert_eq!(result.column_by_name("name").unwrap().len(), 3);

        let select = "use a_database; select * from my_table where id = id;";
        let result = storage.execute(select).await.unwrap();
        let result = match result.get(0).unwrap() {
            Output::Select(results) => results.get(0).unwrap(),
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().field(0).name(), "id");
        assert_eq!(result.schema().field(1).name(), "name");
        storage
            .execute("use a_database; drop table my_table;")
            .await
            .unwrap();
        let _ = storage.execute("drop database a_database;").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    pub async fn multi_file_select() {
        let storage = Storage::new().unwrap();
        let _ = storage.execute("use multi_file_db; drop table t;").await;
        let _ = storage.execute("drop database multi_file_db;").await;
        storage
            .execute("create database multi_file_db; use multi_file_db; create table t (id int32, name string);")
            .await
            .unwrap();

        let table = TableIdent::new(
            NamespaceIdent::new("multi_file_db".to_string()),
            "t".to_string(),
        );
        storage
            .write(table.clone(), vec![test_batch()])
            .await
            .unwrap();
        storage
            .write(table.clone(), vec![test_batch()])
            .await
            .unwrap();

        let output = storage
            .execute("use multi_file_db; select name from t where id = 4;")
            .await
            .unwrap();
        let batches = match output.get(0).unwrap() {
            Output::Select(batches) => batches,
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
        for batch in batches {
            assert_eq!(batch.num_columns(), 1);
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..names.len() {
                assert_eq!(names.value(row), "a");
            }
        }

        let output = storage
            .execute("use multi_file_db; select id, name from t;")
            .await
            .unwrap();
        let batches = match output.get(0).unwrap() {
            Output::Select(batches) => batches,
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            6
        );
        assert_eq!(batches[0].schema().field(0).name(), "id");
        assert_eq!(batches[0].schema().field(1).name(), "name");

        storage
            .execute("use multi_file_db; drop table t;")
            .await
            .unwrap();
        let _ = storage.execute("drop database multi_file_db;").await;
    }

    #[tokio::test]
    #[serial_test::serial]
    pub async fn insert_values() {
        let storage = Storage::new().unwrap();
        let _ = storage.execute("use insert_db; drop table t;").await;
        let _ = storage.execute("drop database insert_db;").await;

        let output = storage
            .execute(
                "create database insert_db; use insert_db; create table t (id int32, name string);\
                 insert into t values (1, 'a'), (2, 'b');",
            )
            .await
            .unwrap();
        assert_eq!(
            output.last().unwrap(),
            &Output::Inserted {
                table: "t".to_string(),
                rows: 2
            }
        );

        let table = TableIdent::new(
            NamespaceIdent::new("insert_db".to_string()),
            "t".to_string(),
        );
        let loaded = storage.catalog.load_table(&table).await.unwrap();
        assert_eq!(loaded.metadata().snapshots().count(), 1);

        let output = storage
            .execute("use insert_db; select id, name from t;")
            .await
            .unwrap();
        let batch = match output.get(0).unwrap() {
            Output::Select(batches) => batches.get(0).unwrap(),
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "b");

        // Omitted columns become NULL.
        let output = storage
            .execute("use insert_db; insert into t (name) values ('c');")
            .await
            .unwrap();
        assert_eq!(
            output.get(0).unwrap(),
            &Output::Inserted {
                table: "t".to_string(),
                rows: 1
            }
        );
        let output = storage
            .execute("use insert_db; select id, name from t;")
            .await
            .unwrap();
        let batch = match output.get(0).unwrap() {
            Output::Select(batches) => batches,
            other => panic!("expected select results, got {other:?}"),
        };
        assert_eq!(batch.iter().map(|batch| batch.num_rows()).sum::<usize>(), 3);
        let mut saw_null_id = false;
        for batch in batch {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            saw_null_id |= (0..ids.len()).any(|row| ids.is_null(row));
        }
        assert!(saw_null_id);

        // Bad row arity is a bind error, not a panic.
        assert!(storage
            .execute("use insert_db; insert into t values (1);")
            .await
            .is_err());

        storage
            .execute("use insert_db; drop table t;")
            .await
            .unwrap();
        let _ = storage.execute("drop database insert_db;").await;
    }
}
