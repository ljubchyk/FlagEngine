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

pub fn mark_batch(tx: &Transaction, seqs: &[i64], status: OutboxStatus) -> Result<()> {
    if seqs.is_empty() {
        return Ok(());
    }

    let mut stmt = tx.prepare_cached(
        "UPDATE outbox_events SET status = ?2 WHERE seq = ?1 AND status = 'Pending'",
    )?;
    let mut changed = 0;
    for seq in seqs {
        changed += stmt.execute(params![seq, status])?;
    }

    if changed != seqs.len() {
        return Err(rusqlite::Error::StatementChangedRows(changed));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::EventPayload;

    fn setup() -> Connection {
        let conn = super::super::db::init(":memory:").unwrap();
        conn
    }

    fn event(key: &str, enabled: bool, at: i64) -> DomainEvent {
        DomainEvent {
            key: key.into(),
            actor: "test".into(),
            occurred_at: at,
            payload: EventPayload::FlagToggled {
                is_enabled: enabled,
            },
        }
    }

    #[test]
    fn enqueue_fetch_mark_roundtrip() {
        let mut conn = setup();

        let tx = conn.transaction().unwrap();
        let s1 = enqueue(&tx, &event("a", true, 1)).unwrap();
        let s2 = enqueue(&tx, &event("b", false, 2)).unwrap();
        tx.commit().unwrap();
        assert!(s2 > s1);

        let pending = fetch_pending(&conn, 10).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].0, s1);
        assert_eq!(pending[0].1.as_ref().unwrap().key, "a");

        let tx = conn.transaction().unwrap();
        mark_batch(&tx, &[s1], OutboxStatus::Completed).unwrap();
        tx.commit().unwrap();

        let pending = fetch_pending(&conn, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, s2);
    }

    #[test]
    fn unparseable_payload_keeps_seq_and_can_be_marked_failed() {
        let mut conn = setup();
        conn.execute(
            "INSERT INTO outbox_events (payload, created_at) VALUES ('not json', 1)",
            [],
        )
        .unwrap();

        let pending = fetch_pending(&conn, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].1.is_err());

        let tx = conn.transaction().unwrap();
        mark_batch(&tx, &[pending[0].0], OutboxStatus::Failed).unwrap();
        tx.commit().unwrap();
        assert!(fetch_pending(&conn, 10).unwrap().is_empty());
    }

    #[test]
    fn mark_unknown_seq_is_an_error() {
        let mut conn = setup();
        let tx = conn.transaction().unwrap();
        assert!(mark_batch(&tx, &[999], OutboxStatus::Completed).is_err());
    }
}
