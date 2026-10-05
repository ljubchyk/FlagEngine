use crate::domain::{DomainEvent, FeatureFlag};
use rusqlite::{
    Connection, Error, OptionalExtension, Result, Row, ToSql, Transaction, params,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};

pub enum OutboxStatus {
    Completed,
    Failed,
}

impl ToSql for OutboxStatus {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let raw = match self {
            OutboxStatus::Completed => "Completed",
            OutboxStatus::Failed => "Failed",
        };

        Ok(ToSqlOutput::from(raw))
    }
}

impl ToSql for DomainEvent {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let json =
            serde_json::to_string(self).map_err(|e| Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(json))
    }
}

impl FromSql for DomainEvent {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let text = value.as_str()?;
        serde_json::from_str(text).map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

#[derive(Default)]
pub struct SqliteOutboxRepository;

impl SqliteOutboxRepository {
    pub fn enqueue(&self, tx: &Transaction, event: &DomainEvent) -> Result<i64> {
        tx.execute(
            "INSERT INTO outbox_events (payload, status, created_at) VALUES (?1, 'Pending', ?2)",
            params![event, event.occurred_at],
        )?;

        Ok(tx.last_insert_rowid())
    }

    pub fn fetch_pending(
        &self,
        conn: &Connection,
        limit: u32,
    ) -> Result<Vec<(i64, Result<DomainEvent>)>> {
        let mut stmt = conn.prepare_cached(
            "SELECT seq, payload FROM outbox_events WHERE status = 'Pending' ORDER BY seq LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit], |row| {
            let seq: i64 = row.get(0)?;
            let event: Result<DomainEvent> = row.get(1);

            Ok((seq, event))
        })?;

        rows.collect()
    }

    pub fn mark(&self, tx: &Transaction, seq: i64, status: OutboxStatus) -> Result<()> {
        let changed = tx.execute(
            "UPDATE outbox_events SET status = ?2 WHERE seq = ?1",
            params![seq, status],
        )?;
        if changed != 1 {
            return Err(Error::StatementChangedRows(changed));
        }

        Ok(())
    }
}

#[derive(Default)]
pub enum FlagFilter {
    #[default]
    Active,
    Archived,
    All,
}

pub struct SqliteFlagRepository;

impl SqliteFlagRepository {
    fn raw_to_flag(row: &Row) -> Result<FeatureFlag> {
        Ok(FeatureFlag::restore(
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
        ))
    }

    pub fn new() -> Self {
        Self
    }

    pub fn save(&self, tx: &Transaction, flag: &FeatureFlag) -> Result<()> {
        tx.execute(
            "INSERT INTO feature_flags (key, is_enabled, is_archived, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                flag.key(),
                flag.is_enabled(),
                flag.is_archived(),
                flag.updated_at()
            ],
        )?;
        Ok(())
    }

    pub fn find(&self, conn: &Connection, key: &str) -> Result<Option<FeatureFlag>> {
        conn.prepare_cached(
            "SELECT key, is_enabled, is_archived, updated_at FROM feature_flags WHERE key = ?1",
        )?
        .query_row(params![key], Self::raw_to_flag)
        .optional()
    }

    pub fn find_all(&self, conn: &Connection, filter: FlagFilter) -> Result<Vec<FeatureFlag>> {
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
            .query_map([], Self::raw_to_flag)?
            .collect()
    }
}
