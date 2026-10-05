use crate::domain::DomainEvent;
use rusqlite::{
    Connection, Error, Result, ToSql, Transaction, params,
    types::ToSqlOutput,
    types::{FromSql, FromSqlError, FromSqlResult, ValueRef},
};

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

pub fn enqueue(tx: &Transaction, event: &DomainEvent) -> Result<i64> {
    tx.execute(
        "INSERT INTO outbox_events (payload, status, created_at) VALUES (?1, 'Pending', ?2)",
        params![event, event.occurred_at],
    )?;

    Ok(tx.last_insert_rowid())
}

pub fn fetch_pending(conn: &Connection, limit: u32) -> Result<Vec<(i64, Result<DomainEvent>)>> {
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

pub fn mark(tx: &Transaction, seq: i64, status: OutboxStatus) -> Result<()> {
    let changed = tx.execute(
        "UPDATE outbox_events SET status = ?2 WHERE seq = ?1",
        params![seq, status],
    )?;
    if changed != 1 {
        return Err(Error::StatementChangedRows(changed));
    }

    Ok(())
}
