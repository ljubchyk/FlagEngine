use std::sync::Arc;

use crate::cache::FlagCache;
use crate::handlers::notify_sync_subscribers;
use crate::{db::DbPool, domain::FeatureFlag};
use crate::{domain, flag_repo};
use rusqlite::TransactionBehavior;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("Feature flag with key '{0}' was not found")]
    FlagNotFound(String),

    #[error("Feature flag with key '{0}' already exists")]
    DuplicateKey(String),

    #[error("Database operation failed: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Failed to acquire connection from pool: {0}")]
    Pool(#[from] r2d2::Error),

    #[error("...")]
    Domain(#[from] domain::DomainError),
}

pub type Result<T> = std::result::Result<T, ServiceError>;

fn is_unique_violation(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(inner, _) => {
            inner.code == rusqlite::ErrorCode::ConstraintViolation && inner.extended_code == 1555
        }
        _ => false,
    }
}

pub struct FeatureFlagService {
    pool: DbPool,
    cache: Arc<FlagCache>,
}

impl FeatureFlagService {
    pub fn new(pool: DbPool, cache: Arc<FlagCache>) -> Self {
        Self { pool, cache }
    }

    pub fn is_enabled(&self, key: &str) -> bool {
        self.cache.is_enabled(key)
    }

    pub fn create_flag(&self, key: String, actor: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let (flag, event) = FeatureFlag::create(key.clone(), actor)?;

        flag_repo::save(&tx, &flag).map_err(|err| {
            if is_unique_violation(&err) {
                ServiceError::DuplicateKey(key)
            } else {
                ServiceError::Database(err)
            }
        })?;
        notify_sync_subscribers(&tx, &event)?;

        tx.commit()?;
        Ok(())
    }

    pub fn toggle_flag(&self, key: String, enabled: bool, actor: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut flag = flag_repo::find(&tx, &key)?.ok_or(ServiceError::FlagNotFound(key))?;

        if let Some(event) = flag.set_enabled(enabled, actor)? {
            flag_repo::save(&tx, &flag)?;
            notify_sync_subscribers(&tx, &event)?;

            tx.commit()?;
        }

        Ok(())
    }

    pub fn archive_flag(&self, key: String, actor: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut flag = flag_repo::find(&tx, &key)?.ok_or(ServiceError::FlagNotFound(key))?;

        if let Some(event) = flag.archive(actor) {
            flag_repo::save(&tx, &flag)?;
            notify_sync_subscribers(&tx, &event)?;

            tx.commit()?;
        }

        Ok(())
    }
}
