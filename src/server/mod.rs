use std::path::PathBuf;

use crate::backend::Storage;
use crate::error::Error;

pub struct Server {
    storage: Storage,
}

impl Server {
    pub fn new(storage_path: PathBuf) -> Result<Self, Error> {
        Ok(Self {
            storage: Storage::new()?,
        })
    }

    pub async fn execute(&self, sql: String) -> Result<(), Error> {
        let _result = self.storage.execute(&sql).await?;
        todo!()
    }
}
