use arrow::error::ArrowError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to parse sql: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),
    // TODO we should be more helpful
    #[error("the operation is not supported: {0}")]
    NotSupportedSql(String),

    #[error("An IO error happened: {0}")]
    IOError(#[from] std::io::Error),

    #[error("An error while executing iceberg commands: {0}")]
    IcebergError(#[from] iceberg::Error),

    #[error("Database already exist: {0}")]
    DatabaseAlreadyExist(String),

    #[error("Item does not exist: {0}")]
    NotPossibletoDrop(String),

    #[error("Error while using Arrow: {0}")]
    ArrowError(#[from] ArrowError),
}
