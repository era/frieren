use crate::error::Error;
use crate::frontend::Query;
use iceberg::{io::FileIOBuilder, Catalog};
use iceberg::{Namespace, NamespaceIdent};
use iceberg_catalog_memory::MemoryCatalog;
use okaywal::{Entry, EntryId, LogManager, SegmentReader, WriteAheadLog};
use sqlparser::ast::Use;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub enum Output {
    CreatedDatabase(String),
    Use(Context),
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
    wal: WriteAheadLog,
    catalog: Arc<dyn Catalog>,
}

impl Storage {
    pub fn new(path: PathBuf) -> Result<Self, Error> {
        let catalog = Arc::new(new_memory_catalog()?);
        let wal = WriteAheadLog::recover(
            &path,
            WalRecover {
                catalog: catalog.clone(),
            },
        )?;
        Ok(Self { path, wal, catalog })
    }
    pub async fn execute(&self, sql: Query) -> Result<Vec<Output>, Error> {
        let mut context = Context::default();
        let mut results = Vec::with_capacity(sql.stmts.len());
        let mut writer = self.wal.begin_entry()?;
        for stmt in sql.stmts {
            //FIXME: avoid the allocation
            writer.write_chunk(stmt.to_string().as_bytes())?;
            let result = self.execute_stmt(context.clone(), stmt).await?;
            // if the previous statement was a `use` we should start using that namespace
            if let Output::Use(ctx) = result {
                context = ctx;
            } else {
                // otherwise we should push the results to be retrieved by the user later
                results.push(result);
            }
        }
        //FIXME: should handle the rollback
        //FIXME: For now, assuming we will autocommit per query send to the server
        // and not respecting rollback / commits / begin transaction from users
        writer.commit()?;
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
            e => Err(Error::NotSupportedSql(e.to_string())),
        }
    }

    async fn use_stmt(&self, ctx: Context, u: Use) -> Result<Context, Error> {
        match u {
            // for us both means the same
            Use::Schema(name) | Use::Database(name) => {
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
        match self.catalog.get_namespace(&namespace_ident).await {
            // we probably should keep a catalog and check for this type of errors
            // on the frontend
            Ok(namespace) if !if_not_exists => Err(Error::DatabaseAlreadyExist(
                namespace.name().to_url_string(),
            )),
            //FIXME: Should really check error, but let's for now just assume it means the namespace do not exist
            Err(_) => Ok(self
                .catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await?),
            Ok(namespace) => Ok(namespace), // if already exists, but `if_not_exists` just keep going
        }
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

fn new_memory_catalog() -> Result<MemoryCatalog, Error> {
    let file_io = FileIOBuilder::new_fs_io().build()?;
    Ok(MemoryCatalog::new(file_io, None))
}

#[derive(Debug)]
struct WalRecover {
    catalog: Arc<dyn Catalog>,
}

impl LogManager for WalRecover {
    fn recover(&mut self, _entry: &mut Entry<'_>) -> std::io::Result<()> {
        //TODO
        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        _last_checkpointed_id: EntryId,
        _checkpointed_entries: &mut SegmentReader,
        _wal: &WriteAheadLog,
    ) -> std::io::Result<()> {
        //TODO
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::frontend::parse;

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
}
