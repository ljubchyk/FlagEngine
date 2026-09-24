use crate::handlers::process_events;
use crate::repository::SqliteFlagRepository;
use crate::{db::DbPool, domain::FeatureFlag};
use rusqlite::TransactionBehavior;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("Feature flag with ID '{0}' was not found")]
    FlagNotFound(Uuid),

    #[error("Feature flag with key '{0}' was not found")]
    FlagKeyNotFound(String),

    // #[error("Invalid rule configuration for flag '{key}': {reason}")]
    // InvalidRule { key: String, reason: String },
    #[error("Database operation failed: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Failed to acquire connection from pool: {0}")]
    Pool(#[from] r2d2::Error),
}

pub type Result<T> = std::result::Result<T, ServiceError>;

pub struct FeatureFlagService {
    pool: DbPool,
    repo: SqliteFlagRepository,
}

impl FeatureFlagService {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            repo: SqliteFlagRepository::new(),
        }
    }

    pub fn create_flag(&self, key: String, actor_id: String) -> Result<Uuid> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let flag = FeatureFlag::new(key, actor_id);
        let flag_id = flag.id;

        self.repo.save(&tx, &flag)?;
        process_events(&tx, &flag.domain_events)?;

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
        process_events(&tx, &flag.domain_events)?;

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
        process_events(&tx, &flag.domain_events)?;

        tx.commit()?;
        Ok(())
    }
}
