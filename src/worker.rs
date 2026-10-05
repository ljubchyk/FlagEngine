use rusqlite::{Connection, Result};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{thread, time::Duration};

use crate::cache::FlagCache;
use crate::db::apply_parameters;
use crate::handlers::notify_async_subscribers;
use crate::outbox_repo;

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
        }
    });

    Ok(handle)
}

fn process_pending_messages(conn: &mut Connection, cache: &FlagCache) -> Result<usize> {
    let rows = outbox_repo::fetch_pending(conn, 50)?;
    if rows.is_empty() {
        return Ok(0);
    }

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

    let mut processed_count = 0;

    for (seq, event) in rows {
        match event {
            Ok(event) => {
                notify_async_subscribers(&event, cache);
                outbox_repo::mark(&tx, seq, outbox_repo::OutboxStatus::Completed)?;
            }
            Err(e) => {
                eprintln!("[OutboxWorker] Failed to parse event: {}", e);
                outbox_repo::mark(&tx, seq, outbox_repo::OutboxStatus::Failed)?;
            }
        }

        processed_count += 1;
    }

    tx.commit()?;
    Ok(processed_count)
}
