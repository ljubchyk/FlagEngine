use crate::domain::{DomainEvent, EventPayload};
use rusqlite::{
    Error, Result, ToSql, Transaction, params,
    types::ToSqlOutput,
    types::{FromSql, FromSqlError, FromSqlResult, ValueRef},
};

impl ToSql for EventPayload {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let json =
            serde_json::to_string(self).map_err(|e| Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(json))
    }
}

impl FromSql for EventPayload {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let text = value.as_str()?;
        serde_json::from_str(text).map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

fn action(payload: &EventPayload) -> &'static str {
    match payload {
        EventPayload::FlagCreated { .. } => "FlagCreated",
        EventPayload::FlagToggled { .. } => "FlagToggled",
        EventPayload::FlagArchived => "FlagArchived",
    }
}

pub fn append(tx: &Transaction, event: &DomainEvent) -> Result<()> {
    tx.execute(
        "INSERT INTO audit (key, actor, action, payload, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event.key,
            event.actor,
            action(&event.payload),
            event.payload,
            event.occurred_at
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use rusqlite::Connection;

    fn setup() -> Connection {
        db::init(":memory:").unwrap()
    }

    fn event(key: &str, actor: &str, payload: EventPayload, at: i64) -> DomainEvent {
        DomainEvent {
            key: key.into(),
            actor: actor.into(),
            payload,
            occurred_at: at,
        }
    }

    fn toggled(key: &str, enabled: bool, at: i64) -> DomainEvent {
        event(
            key,
            "alice",
            EventPayload::FlagToggled {
                is_enabled: enabled,
            },
            at,
        )
    }

    fn append_committed(conn: &mut Connection, e: &DomainEvent) {
        let tx = conn.transaction().unwrap();
        append(&tx, e).unwrap();
        tx.commit().unwrap();
    }

    fn count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM audit", [], |r| r.get(0))
            .unwrap()
    }

    type Row = (i64, String, String, String, String, i64);

    fn rows(conn: &Connection) -> Vec<Row> {
        conn.prepare("SELECT id, key, actor, action, payload, created_at FROM audit ORDER BY id")
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            })
            .unwrap()
            .collect::<Result<_>>()
            .unwrap()
    }

    // ---------- action ----------

    #[test]
    fn action_names_match_payload_variants() {
        assert_eq!(
            action(&EventPayload::FlagCreated { is_enabled: false }),
            "FlagCreated"
        );
        assert_eq!(
            action(&EventPayload::FlagToggled { is_enabled: true }),
            "FlagToggled"
        );
        assert_eq!(action(&EventPayload::FlagArchived), "FlagArchived");
    }

    // ---------- append ----------

    #[test]
    fn append_stores_all_fields() {
        let mut conn = setup();
        append_committed(&mut conn, &toggled("checkout", true, 1234));

        let r = rows(&conn);
        assert_eq!(r.len(), 1);
        let (id, key, actor, act, payload, at) = &r[0];
        assert!(*id >= 1);
        assert_eq!(key, "checkout");
        assert_eq!(actor, "alice");
        assert_eq!(act, "FlagToggled");
        assert_eq!(*at, 1234);
        let json: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(json["type"], "FlagToggled");
        assert_eq!(json["is_enabled"], true);
    }

    #[test]
    fn append_stores_each_variant_with_matching_action() {
        let mut conn = setup();
        let payloads = [
            (
                EventPayload::FlagCreated { is_enabled: false },
                "FlagCreated",
            ),
            (
                EventPayload::FlagToggled { is_enabled: true },
                "FlagToggled",
            ),
            (EventPayload::FlagArchived, "FlagArchived"),
        ];
        for (i, (p, _)) in payloads.iter().enumerate() {
            append_committed(&mut conn, &event("k", "bob", p.clone(), i as i64));
        }

        let r = rows(&conn);
        assert_eq!(r.len(), 3);
        for (row, (_, expected)) in r.iter().zip(payloads.iter()) {
            assert_eq!(row.3, *expected);
        }
    }

    #[test]
    fn append_is_append_only_and_ids_increase() {
        let mut conn = setup();
        append_committed(&mut conn, &toggled("a", true, 1));
        append_committed(&mut conn, &toggled("a", false, 2));
        append_committed(&mut conn, &toggled("a", true, 3));

        let r = rows(&conn);
        assert_eq!(r.len(), 3);
        assert!(r[0].0 < r[1].0 && r[1].0 < r[2].0);
        assert_eq!(r.iter().map(|x| x.5).collect::<Vec<_>>(), [1, 2, 3]);
    }

    #[test]
    fn identical_events_are_not_deduplicated() {
        let mut conn = setup();
        let e = toggled("a", true, 1);
        append_committed(&mut conn, &e);
        append_committed(&mut conn, &e);
        assert_eq!(count(&conn), 2);
    }

    #[test]
    fn append_keeps_history_per_key() {
        let mut conn = setup();
        append_committed(&mut conn, &toggled("a", true, 1));
        append_committed(&mut conn, &toggled("b", true, 2));
        append_committed(&mut conn, &toggled("a", false, 3));

        let a: i64 = conn
            .query_row("SELECT COUNT(*) FROM audit WHERE key = 'a'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(a, 2);
    }

    #[test]
    fn multiple_appends_in_one_transaction_commit_together() {
        let mut conn = setup();
        let tx = conn.transaction().unwrap();
        append(&tx, &toggled("a", true, 1)).unwrap();
        append(&tx, &toggled("b", true, 2)).unwrap();
        tx.commit().unwrap();
        assert_eq!(count(&conn), 2);
    }

    #[test]
    fn rolled_back_transaction_leaves_no_audit_rows() {
        let mut conn = setup();
        let tx = conn.transaction().unwrap();
        append(&tx, &toggled("a", true, 1)).unwrap();
        tx.rollback().unwrap();
        assert_eq!(count(&conn), 0);
    }

    #[test]
    fn dropped_transaction_leaves_no_audit_rows() {
        let mut conn = setup();
        {
            let tx = conn.transaction().unwrap();
            append(&tx, &toggled("a", true, 1)).unwrap();
        }
        assert_eq!(count(&conn), 0);
    }

    #[test]
    fn append_does_not_require_existing_flag() {
        // audit не має FK на flags: лог переживає видалення/відсутність прапорця
        let mut conn = setup();
        append_committed(&mut conn, &toggled("ghost", true, 1));
        assert_eq!(count(&conn), 1);
    }

    #[test]
    fn append_supports_unicode_actor() {
        let mut conn = setup();
        let e = event("a", "Олена", EventPayload::FlagArchived, 1);
        append_committed(&mut conn, &e);
        assert_eq!(rows(&conn)[0].2, "Олена");
    }

    // ---------- ToSql / FromSql ----------

    fn read_payload(conn: &Connection) -> Result<EventPayload> {
        conn.query_row("SELECT payload FROM audit LIMIT 1", [], |r| r.get(0))
    }

    #[test]
    fn payload_roundtrips_through_sql() {
        let mut conn = setup();
        for p in [
            EventPayload::FlagCreated { is_enabled: true },
            EventPayload::FlagToggled { is_enabled: false },
            EventPayload::FlagArchived,
        ] {
            conn.execute("DELETE FROM audit", []).unwrap();
            append_committed(&mut conn, &event("k", "a", p.clone(), 1));

            let back = read_payload(&conn).unwrap();
            assert_eq!(
                serde_json::to_value(&back).unwrap(),
                serde_json::to_value(&p).unwrap()
            );
        }
    }

    #[test]
    fn payload_uses_internally_tagged_json() {
        let out = EventPayload::FlagArchived.to_sql().unwrap();
        let ToSqlOutput::Owned(rusqlite::types::Value::Text(s)) = out else {
            panic!("expected owned text");
        };
        assert_eq!(s, r#"{"type":"FlagArchived"}"#);
    }

    #[test]
    fn invalid_json_payload_fails_to_decode() {
        let conn = setup();
        conn.execute(
            "INSERT INTO audit (key, actor, action, payload, created_at) VALUES ('k','a','x','not json',1)",
            [],
        )
        .unwrap();
        assert!(matches!(
            read_payload(&conn),
            Err(Error::FromSqlConversionFailure(..))
        ));
    }

    #[test]
    fn unknown_payload_type_fails_to_decode() {
        let conn = setup();
        conn.execute(
            r#"INSERT INTO audit (key, actor, action, payload, created_at) VALUES ('k','a','x','{"type":"Nope"}',1)"#,
            [],
        )
        .unwrap();
        assert!(read_payload(&conn).is_err());
    }

    #[test]
    fn non_text_payload_fails_to_decode() {
        let conn = setup();
        conn.execute(
            "INSERT INTO audit (key, actor, action, payload, created_at) VALUES ('k','a','x',42,1)",
            [],
        )
        .unwrap();
        assert!(read_payload(&conn).is_err());
    }
}
