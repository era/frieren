use std::path::PathBuf;

use crate::backend::Storage;
use crate::error::Error;
use crate::frontend::parse;

pub struct Server {
    storage: Storage,
}

impl Server {
    pub fn new(storage_path: PathBuf) -> Result<Self, Error> {
        Ok(Self {
            storage: Storage::new(storage_path)?,
        })
    }

    pub async fn execute(&self, sql: String) -> Result<(), Error> {
        let stmts = parse(&sql)?;
        let _result = self.storage.execute(stmts).await?;
        todo!()
    }
}
