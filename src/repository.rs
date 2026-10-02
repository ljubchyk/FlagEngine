use crate::domain::FeatureFlag;
use rusqlite::{Connection, OptionalExtension, Result, Row, Transaction, params};
use uuid::Uuid;

#[derive(Default)]
pub enum FlagFilter {
    #[default]
    Active,
    Archived,
    All,
}

pub struct SqliteFlagRepository;

impl SqliteFlagRepository {
    const COLUMNS: &'static str = "id, key, is_enabled, is_archived, updated_at";

    fn raw_to_flag(row: &Row) -> Result<FeatureFlag> {
        Ok(FeatureFlag::restore(
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
        ))
    }

    pub fn new() -> Self {
        Self
    }

    pub fn save(&self, tx: &Transaction, flag: &FeatureFlag) -> Result<()> {
        tx.execute(
            "INSERT INTO feature_flags (id, key, is_enabled, is_archived, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                is_enabled = excluded.is_enabled,
                is_archived = excluded.is_archived,
                updated_at = excluded.updated_at",
            params![
                flag.id,
                flag.key,
                flag.is_enabled,
                flag.is_archived,
                flag.updated_at
            ],
        )?;
        Ok(())
    }

    pub fn find_by_id(&self, conn: &Connection, id: Uuid) -> Result<Option<FeatureFlag>> {
        let sql = format!("SELECT {} FROM feature_flags WHERE id = ?1", Self::COLUMNS);
        conn.prepare_cached(&sql)?
            .query_row(params![id.to_string()], Self::raw_to_flag)
            .optional()
    }

    pub fn find_by_key(&self, conn: &Connection, key: &str) -> Result<Option<FeatureFlag>> {
        let sql = format!("SELECT {} FROM feature_flags WHERE key = ?1", Self::COLUMNS);
        conn.prepare_cached(&sql)?
            .query_row(params![key], Self::raw_to_flag)
            .optional()
    }

    pub fn find_all(&self, conn: &Connection, filter: FlagFilter) -> Result<Vec<FeatureFlag>> {
        let where_closure = match filter {
            FlagFilter::Active => " WHERE is_archived = 0",
            FlagFilter::Archived => " WHERE is_archived = 1",
            FlagFilter::All => ""
        };
        let sql = format!("SELECT {} FROM feature_flags{}", Self::COLUMNS, where_closure);
        conn.prepare_cached(&sql)?
            .query_map([], Self::raw_to_flag)?
            .collect()
    }
}
