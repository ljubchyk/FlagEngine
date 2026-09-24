use rusqlite::{Connection, Result, params};
use std::thread::JoinHandle;
use std::{sync::Arc, thread, time::Duration};

use crate::domain::{DomainEvent, EventPayload};
use crate::{cache::FlagCache, db::apply_parameters};

pub fn spawn_outbox_worker(
    db_path: &str,
    poll_interval: Duration,
    cache: Arc<FlagCache>,
) -> Result<JoinHandle<()>> {
    let mut conn = Connection::open(&db_path)?;
    apply_parameters(&conn)?;

    let handle = thread::spawn(move || {
        loop {
            if let Err(e) = process_pending_messages(&mut conn, &cache) {
                eprintln!("[OutboxWorker] Error processing batch: {}", e);
            }

            thread::sleep(poll_interval);
        }
    });

    Ok(handle)
}

fn process_pending_messages(conn: &mut Connection, cache: &FlagCache) -> Result<usize> {
    // Вибираємо пачку 'Pending' подій, сортуючи за часом створення (UUIDv7 гарантує хронологію)
    let messages = conn
        .prepare_cached(
            "SELECT id, event_type, payload 
             FROM outbox_messages 
             WHERE status = 'Pending' 
             ORDER BY created_at ASC 
             LIMIT 50",
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    if messages.is_empty() {
        return Ok(0);
    }

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let mut complete_stmt =
        tx.prepare_cached("UPDATE outbox_messages SET status = 'Completed' WHERE id = ?1")?;
    let mut failed_stmt =
        tx.prepare_cached("UPDATE outbox_messages SET status = 'Failed' WHERE id = ?1")?;

    let mut processed_count = 0;

    for (id, event_type, payload) in &messages {
        println!(
            "🚀 [OutboxWorker] Dispatching event -> Type: {}, Payload: {}",
            event_type, payload
        );

        match serde_json::from_str::<DomainEvent>(payload) {
            Ok(event) => {
                match event.payload {
                    EventPayload::FlagCreated { key, is_enabled }
                    | EventPayload::FlagToogled { key, is_enabled } => {
                        cache.update(&key, is_enabled)
                    }
                    EventPayload::FlagArchived { key } => cache.remove(&key),
                }

                complete_stmt.execute(params![id])?;
            }
            Err(e) => {
                eprintln!("[OutboxWorker] Failed to parse event: {}", e);
                eprintln!(
                    "[OutboxWorker] Event {} could not be applied to cache - marking Failed, needs investigation",
                    id
                );

                failed_stmt.execute(params![id])?;
            }
        }

        processed_count += 1;
    }

    drop(complete_stmt);
    drop(failed_stmt);

    tx.commit()?;
    Ok(processed_count)
}
