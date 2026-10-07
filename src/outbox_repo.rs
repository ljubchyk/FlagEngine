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
        "INSERT INTO outbox (payload, status, created_at) VALUES (?1, 'Pending', ?2)",
        params![event, event.occurred_at],
    )?;

    Ok(tx.last_insert_rowid())
}

pub fn fetch_pending(conn: &Connection, limit: u32) -> Result<Vec<(i64, Result<DomainEvent>)>> {
    let mut stmt = conn.prepare_cached(
        "SELECT seq, payload FROM outbox WHERE status = 'Pending' ORDER BY seq LIMIT ?1",
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
        "UPDATE outbox SET status = ?2 WHERE seq = ?1 AND status = 'Pending'",
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
            "INSERT INTO outbox (payload, created_at) VALUES ('not json', 1)",
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

    fn status_of(conn: &Connection, seq: i64) -> String {
        conn.query_row(
            "SELECT status FROM outbox WHERE seq = ?1",
            params![seq],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn enqueue_many(conn: &mut Connection, n: i64) -> Vec<i64> {
        let tx = conn.transaction().unwrap();
        let seqs = (1..=n)
            .map(|i| enqueue(&tx, &event(&format!("flag-{i}"), true, i)).unwrap())
            .collect();
        tx.commit().unwrap();
        seqs
    }

    #[test]
    fn enqueue_stores_pending_status_and_event_timestamp() {
        let mut conn = setup();
        let tx = conn.transaction().unwrap();
        let seq = enqueue(&tx, &event("a", true, 42)).unwrap();
        tx.commit().unwrap();

        let (status, created_at): (String, i64) = conn
            .query_row(
                "SELECT status, created_at FROM outbox WHERE seq = ?1",
                params![seq],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "Pending");
        assert_eq!(created_at, 42);
    }

    #[test]
    fn fetch_pending_is_ordered_by_seq_and_respects_limit() {
        let mut conn = setup();
        let seqs = enqueue_many(&mut conn, 5);

        let page = fetch_pending(&conn, 3).unwrap();
        let got: Vec<i64> = page.iter().map(|(s, _)| *s).collect();
        assert_eq!(got, seqs[..3]);
        assert_eq!(page[0].1.as_ref().unwrap().key, "flag-1");
    }

    #[test]
    fn fetch_pending_skips_completed_and_failed() {
        let mut conn = setup();
        let seqs = enqueue_many(&mut conn, 3);

        let tx = conn.transaction().unwrap();
        mark_batch(&tx, &seqs[..1], OutboxStatus::Completed).unwrap();
        mark_batch(&tx, &seqs[1..2], OutboxStatus::Failed).unwrap();
        tx.commit().unwrap();

        assert_eq!(status_of(&conn, seqs[0]), "Completed");
        assert_eq!(status_of(&conn, seqs[1]), "Failed");

        let pending = fetch_pending(&conn, 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, seqs[2]);
    }

    #[test]
    fn mark_batch_with_empty_slice_is_noop() {
        let mut conn = setup();
        let tx = conn.transaction().unwrap();
        mark_batch(&tx, &[], OutboxStatus::Completed).unwrap();
    }

    #[test]
    fn mark_batch_cannot_change_already_processed_event() {
        let mut conn = setup();
        let seqs = enqueue_many(&mut conn, 1);

        let tx = conn.transaction().unwrap();
        mark_batch(&tx, &seqs, OutboxStatus::Completed).unwrap();
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        let err = mark_batch(&tx, &seqs, OutboxStatus::Failed).unwrap_err();
        assert!(matches!(err, rusqlite::Error::StatementChangedRows(0)));
        drop(tx);

        assert_eq!(status_of(&conn, seqs[0]), "Completed");
    }

    #[test]
    fn failed_batch_rolls_back_when_transaction_is_dropped() {
        let mut conn = setup();
        let seqs = enqueue_many(&mut conn, 1);

        let tx = conn.transaction().unwrap();
        // перший seq валідний, другий не існує: batch має провалитись
        assert!(mark_batch(&tx, &[seqs[0], 999], OutboxStatus::Completed).is_err());
        drop(tx); // без commit -> rollback

        assert_eq!(status_of(&conn, seqs[0]), "Pending");
        assert_eq!(fetch_pending(&conn, 10).unwrap().len(), 1);
    }

    #[test]
    fn rolled_back_transaction_leaves_no_outbox_rows() {
        let mut conn = setup();

        let tx = conn.transaction().unwrap();
        enqueue(&tx, &event("a", true, 1)).unwrap();
        drop(tx);

        assert!(fetch_pending(&conn, 10).unwrap().is_empty());
    }

    #[test]
    fn all_payload_variants_survive_the_roundtrip() {
        let mut conn = setup();
        let tx = conn.transaction().unwrap();
        for payload in [
            EventPayload::FlagCreated { is_enabled: false },
            EventPayload::FlagToggled { is_enabled: true },
            EventPayload::FlagArchived,
        ] {
            let e = DomainEvent {
                key: "k".into(),
                actor: "alice".into(),
                payload,
                occurred_at: 7,
            };
            enqueue(&tx, &e).unwrap();
        }
        tx.commit().unwrap();

        let events: Vec<DomainEvent> = fetch_pending(&conn, 10)
            .unwrap()
            .into_iter()
            .map(|(_, e)| e.unwrap())
            .collect();

        assert!(matches!(
            events[0].payload,
            EventPayload::FlagCreated { is_enabled: false }
        ));
        assert!(matches!(
            events[1].payload,
            EventPayload::FlagToggled { is_enabled: true }
        ));
        assert!(matches!(events[2].payload, EventPayload::FlagArchived));
        assert!(
            events
                .iter()
                .all(|e| e.actor == "alice" && e.occurred_at == 7)
        );
    }

    #[test]
    fn schema_rejects_unknown_status() {
        let conn = setup();
        let res = conn.execute(
            "INSERT INTO outbox (status, payload, created_at) VALUES ('Bogus', '{}', 1)",
            [],
        );
        assert!(res.is_err());
    }
}
