use std::sync::Arc;

use crate::cache::FlagCache;
use crate::handlers::notify_sync_subscribers;
use crate::repository::SqliteFlagRepository;
use crate::{db::DbPool, domain::FeatureFlag};
use rusqlite::{TransactionBehavior};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("Feature flag with ID '{0}' was not found")]
    FlagNotFound(Uuid),

    #[error("Feature flag with key '{0}' was not found")]
    FlagKeyNotFound(String),

    #[error("Feature flag with key '{0}' already exists")]
    DuplicateKey(String),

    #[error("Database operation failed: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Failed to acquire connection from pool: {0}")]
    Pool(#[from] r2d2::Error),
}

pub type Result<T> = std::result::Result<T, ServiceError>;

fn is_unique_violation(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(inner, _) => {
            inner.code == rusqlite::ErrorCode::ConstraintViolation && inner.extended_code == 2067 
        },
        _ => false
    }
}

pub struct FeatureFlagService {
    pool: DbPool,
    repo: SqliteFlagRepository,
    cache: Arc<FlagCache>
}

impl FeatureFlagService {
    pub fn new(pool: DbPool, cache: Arc<FlagCache>) -> Self {
        Self {
            pool,
            repo: SqliteFlagRepository::new(),
            cache
        }
    }

    pub fn is_enabled(&self, key: &str) -> bool {
        self.cache.is_enabled(key)
    }

    pub fn create_flag(&self, key: String, actor_id: String) -> Result<Uuid> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let flag = FeatureFlag::new(key.clone(), actor_id);
        let flag_id = flag.id;

        self.repo.save(&tx, &flag).map_err(|err| {
            if is_unique_violation(&err) {
                ServiceError::DuplicateKey(key)
            } else {
                ServiceError::Database(err)
            }
        })?;
        notify_sync_subscribers(&tx, &flag.domain_events)?;

        tx.commit()?;
        Ok(flag_id)
    }

    pub fn toggle_flag(&self, flag_id: Uuid, actor_id: String, new_state: bool) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut flag = self
            .repo
            .find_by_id(&tx, flag_id)?
            .ok_or(ServiceError::FlagNotFound(flag_id))?;
        flag.toggle(actor_id, new_state);

        self.repo.save(&tx, &flag)?;
        notify_sync_subscribers(&tx, &flag.domain_events)?;

        tx.commit()?;
        Ok(())
    }

    pub fn archive_flag(&self, flag_id: Uuid, actor_id: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut flag = self
            .repo
            .find_by_id(&tx, flag_id)?
            .ok_or(ServiceError::FlagNotFound(flag_id))?;
        flag.archive(actor_id);

        self.repo.save(&tx, &flag)?;
        notify_sync_subscribers(&tx, &flag.domain_events)?;

        tx.commit()?;
        Ok(())
    }
}
