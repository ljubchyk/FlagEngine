use crate::domain::FeatureFlag;
use rusqlite::{Connection, OptionalExtension, Result, Transaction, params};
use uuid::Uuid;

pub struct SqliteFlagRepository;

impl SqliteFlagRepository {
    pub fn new() -> Self {
        Self
    }

    pub fn save(&self, tx: &Transaction, flag: &FeatureFlag) -> Result<()> {
        tx.execute(
            "INSERT INTO feature_flags (id, key, is_enabled, is_archived, version, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(id) DO UPDATE SET
                key = excluded.key,
                is_enabled = excluded.is_enabled,
                is_archived = excluded.is_archived,
                updated_at = excluded.updated_at",
            params![
                flag.id.to_string(),
                flag.key,
                flag.is_enabled,
                flag.is_archived,
                flag.updated_at
            ],
        )?;
        Ok(())
    }

    pub fn find_by_id(&self, conn: &Connection, id: Uuid) -> Result<Option<FeatureFlag>> {
        let mut stmt = conn.prepare_cached(
            "SELECT id, key, is_enabled, is_archived, version, updated_at 
             FROM feature_flags WHERE id = ?1",
        )?;

        stmt.query_row(params![id.to_string()], |row| {
            let id_str: String = row.get(0)?;
            Ok(FeatureFlag {
                id: Uuid::parse_str(&id_str).unwrap_or(id),
                key: row.get(1)?,
                is_enabled: row.get(2)?,
                is_archived: row.get(3)?,
                updated_at: row.get(5)?,
                domain_events: Vec::new(),
            })
        })
        .optional()
    }

    pub fn find_by_key(&self, conn: &Connection, key: &str) -> Result<Option<FeatureFlag>> {
        let mut stmt = conn.prepare_cached(
            "SELECT id, key, is_enabled, is_archived, version, updated_at
             FROM feature_flags WHERE key = ?1",
        )?;

        stmt.query_row(params![key], |row| {
            let id_str: String = row.get(0)?;
            Ok(FeatureFlag {
                id: Uuid::parse_str(&id_str).unwrap_or_default(),
                key: row.get(1)?,
                is_enabled: row.get(2)?,
                is_archived: row.get(3)?,
                updated_at: row.get(5)?,
                domain_events: Vec::new(),
            })
        })
        .optional()
    }

    pub fn find_all_active(&self, conn: &Connection) -> Result<Vec<FeatureFlag>> {
        let mut stmt = conn.prepare_cached(
            "SELECT id, key, is_enabled, is_archived, version, updated_at
             FROM feature_flags WHERE is_archived = 0",
        )?;

        stmt.query_map([], |row| {
            let id_str: String = row.get(0)?;
            Ok(FeatureFlag {
                id: Uuid::parse_str(&id_str).unwrap_or_default(),
                key: row.get(1)?,
                is_enabled: row.get(2)?,
                is_archived: row.get(3)?,
                updated_at: row.get(5)?,
                domain_events: Vec::new(),
            })
        })?
        .collect()
    }

    pub fn find_all(&self, conn: &Connection) -> Result<Vec<FeatureFlag>> {
        let mut stmt = conn.prepare_cached(
            "SELECT id, key, is_enabled, is_archived, version, updated_at
             FROM feature_flags WHERE is_archived = 0",
        )?;

        stmt.query_map([], |row| {
            let id_str: String = row.get(0)?;
            Ok(FeatureFlag {
                id: Uuid::parse_str(&id_str).unwrap_or_default(),
                key: row.get(1)?,
                is_enabled: row.get(2)?,
                is_archived: row.get(3)?,
                updated_at: row.get(5)?,
                domain_events: Vec::new(),
            })
        })?
        .collect()
    }
}
