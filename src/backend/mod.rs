use crate::error::Error;
use crate::frontend::Query;
use iceberg::{io::FileIOBuilder, Catalog};
use iceberg::{Namespace, NamespaceIdent};
use iceberg_catalog_memory::MemoryCatalog;
use okaywal::{Entry, EntryId, LogManager, SegmentReader, WriteAheadLog};
use sqlparser::ast::Spanned;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub enum Output {
    CreatedDatabase(String),
}

pub struct Context {}

pub struct Storage {
    path: PathBuf,
    // we don't support any isolation between queries,
    // which means we have dirty reads
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
        let mut results = Vec::with_capacity(sql.stmts.len());
        let mut writer = self.wal.begin_entry()?;
        for stmt in sql.stmts {
            //FIXME: avoid the allocation
            writer.write_chunk(stmt.to_string().as_bytes())?;
            results.push(self.execute_stmt(stmt).await?);
        }
        //FIXME: should handle the rollback
        //FIXME: For now, assuming we will autocommit per query send to the server
        // and not respecting rollback / commits / begin transaction from users
        writer.commit()?;
        Ok(results)
    }
    async fn execute_stmt(&self, stmt: sqlparser::ast::Statement) -> Result<Output, Error> {
        match stmt {
            sqlparser::ast::Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(Output::CreatedDatabase(
                self.create_database(Context {}, db_name, if_not_exists)
                    .await?
                    .name()
                    .to_url_string(),
            )),
            e => Err(Error::NotSupportedSql(e.to_string())),
        }
    }

    async fn create_database(
        &self,
        ctx: Context,
        db_name: sqlparser::ast::ObjectName,
        if_not_exists: bool,
    ) -> Result<Namespace, Error> {
        // we use namespace as database, so if the user created a `my_cool_schema.my_cool_db`
        // we will create a namespace my_cool_schema with a subnamespace called `my_cool_db`.
        // FIXME: should look at context to see if the user already used something like `use my_cool_schema;`
        // FIXME: on previous statements
        let namespace_ident = NamespaceIdent::from_vec(
            db_name
                .0
                .into_iter()
                // it's safe to unwrap as_indent because as now there will always be one there
                .map(|i| i.as_ident().unwrap().value.clone())
                .collect(),
        )?;
        //check if exists and what if_not_exists is set to do
        match self.catalog.get_namespace(&namespace_ident).await {
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
    }
}
