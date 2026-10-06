use crate::domain::FeatureFlag;
use rusqlite::{Connection, OptionalExtension, Result, Row, Transaction, params};

#[derive(Default)]
pub enum FlagFilter {
    #[default]
    Active,
    Archived,
    All,
}

fn raw_to_flag(row: &Row) -> Result<FeatureFlag> {
    Ok(FeatureFlag::restore(
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
    ))
}

pub fn save(tx: &Transaction, flag: &FeatureFlag) -> Result<()> {
    tx.execute(
        "INSERT INTO feature_flags (key, is_enabled, is_archived, updated_at)
             VALUES (?1, ?2, ?3, ?4) ON CONFLICT(key) DO UPDATE SET is_enabled = ?2, is_archived = ?3, updated_at = ?4",
        params![
            flag.key(),
            flag.is_enabled(),
            flag.is_archived(),
            flag.updated_at()
        ],
    )?;
    Ok(())
}

pub fn find(conn: &Connection, key: &str) -> Result<Option<FeatureFlag>> {
    conn.prepare_cached(
        "SELECT key, is_enabled, is_archived, updated_at FROM feature_flags WHERE key = ?1",
    )?
    .query_row(params![key], raw_to_flag)
    .optional()
}

pub fn find_all(conn: &Connection, filter: FlagFilter) -> Result<Vec<FeatureFlag>> {
    let where_closure = match filter {
        FlagFilter::Active => " WHERE is_archived = 0",
        FlagFilter::Archived => " WHERE is_archived = 1",
        FlagFilter::All => "",
    };
    let sql = format!(
        "SELECT key, is_enabled, is_archived, updated_at FROM feature_flags{}",
        where_closure
    );
    conn.prepare_cached(&sql)?
        .query_map([], raw_to_flag)?
        .collect()
}
