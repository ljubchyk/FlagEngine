use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("invalid key: {0}")]
    InvalidKey(&'static str),

    #[error("flag is archived")]
    Archived,

    #[error("flag with key '{0}' already exists (it may be archived)")]
    DuplicateKey(String),

    #[error("flag with key '{0}' was not found")]
    FlagNotFound(String),

    #[error("Database operation failed: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Failed to acquire connection from pool: {0}")]
    Pool(#[from] r2d2::Error),

    #[error("blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
