use rusqlite::{Connection, Result, params};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{thread, time::Duration};
use uuid::Uuid;

use crate::cache::FlagCache;
use crate::db::apply_parameters;
use crate::domain::{DomainEvent, EventPayload};
use crate::handlers::notify_async_subscribers;

pub fn spawn_outbox_worker(
    db_path: &str,
    poll_interval: Duration,
    cache: Arc<FlagCache>,
) -> Result<JoinHandle<()>> {
    let mut conn = Connection::open(&db_path)?;
    apply_parameters(&conn)?;

    let handle = thread::spawn(move || {
        loop {
            match process_pending_messages(&mut conn, &cache) {
                Ok(processed_count) => {
                    if processed_count == 0 {
                        thread::sleep(poll_interval);
                    }
                }
                Err(e) => {
                    eprintln!("[OutboxWorker] Error processing batch: {}", e);
                    thread::sleep(poll_interval);
                }
            }
            // if let Err(e) = process_pending_messages(&mut conn) {
            //     eprintln!("[OutboxWorker] Error processing batch: {}", e);
            // }
        }
    });

    Ok(handle)
}

// fn process_pending_messages(conn: &mut Connection, cache: &FlagCache) -> Result<usize> {
//     // Вибираємо пачку 'Pending' подій, сортуючи за часом створення (UUIDv7 гарантує хронологію)
//     let messages = conn
//         .prepare_cached(
//             "SELECT id, event_type, payload
//              FROM outbox_messages
//              WHERE status = 'Pending'
//              ORDER BY created_at ASC
//              LIMIT 50",
//         )?
//         .query_map([], |row| {
//             Ok((
//                 row.get::<_, Uuid>(0)?,
//                 row.get::<_, String>(1)?,
//                 row.get::<_, EventPayload>(2)?,
//             ))
//         })?
//         .collect::<Result<Vec<_>, _>>()?;

//     if messages.is_empty() {
//         return Ok(0);
//     }

//     let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
//     let mut complete_stmt =
//         tx.prepare_cached("UPDATE outbox_messages SET status = 'Completed' WHERE id = ?1")?;
//     let mut failed_stmt =
//         tx.prepare_cached("UPDATE outbox_messages SET status = 'Failed' WHERE id = ?1")?;

//     let mut processed_count = 0;

//     for (id, _, payload) in &messages {
//         match serde_json::from_str::<DomainEvent>(payload) {
//             Ok(event) => {
//                 notify_async_subscribers(&event, cache);
//                 complete_stmt.execute(params![id])?;
//             }
//             Err(e) => {
//                 eprintln!("[OutboxWorker] Failed to parse event: {}", e);
//                 failed_stmt.execute(params![id])?;
//             }
//         }

//         processed_count += 1;
//     }

//     drop(complete_stmt);
//     drop(failed_stmt);

//     tx.commit()?;
//     Ok(processed_count)
// }

fn process_pending_messages(conn: &mut Connection, cache: &FlagCache) -> Result<usize> {
    let domain_event_results = conn
        .prepare_cached(
            "SELECT id, event_type, payload
             FROM outbox_messages 
             WHERE status = 'Pending' 
             ORDER BY created_at ASC 
             LIMIT 50",
        )?
        .query_map([], |row| {
            Ok(DomainEvent {
                id: row.get(0)?,
                flag_id: row.get(1)?,
                actor: row.get(2)?,
                payload: row.get(3)?,
                occurred_at: row.get(4)?,
            })
        })?
        .collect::<Vec<Result<DomainEvent>>>();

    if domain_event_results.is_empty() {
        return Ok(0);
    }

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let mut complete_stmt =
        tx.prepare_cached("UPDATE outbox_messages SET status = 'Completed' WHERE id = ?1")?;
    // let mut failed_stmt =
    //     tx.prepare_cached("UPDATE outbox_messages SET status = 'Failed' WHERE id = ?1")?;

    let mut processed_count = 0;

    for domain_event_result in &domain_event_results {
        match domain_event_result {
            Ok(domain_event) => {
                notify_async_subscribers(&domain_event, cache);
                complete_stmt.execute(params![domain_event.id])?;
            }
            Err(e) => {
                eprintln!("[OutboxWorker] Failed to parse event: {}", e);
                // failed_stmt.execute(params![id])?;
            }
        }

        processed_count += 1;
    }

    drop(complete_stmt);
    // drop(failed_stmt);

    tx.commit()?;
    Ok(processed_count)
}
