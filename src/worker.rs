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
                Ok(is_processed) => {
                    if is_processed {
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

fn process_pending_messages(conn: &mut Connection, cache: &FlagCache) -> Result<bool> {
    let rows = outbox_repo::fetch_pending(conn, 50)?;
    if rows.is_empty() {
        return Ok(false);
    }

    let mut is_processed = false;
    let mut completed_seqs = Vec::with_capacity(rows.len());
    let mut failed_seqs = Vec::with_capacity(rows.len());

    for (seq, event) in rows {
        match event {
            Ok(event) => {
                notify_async_subscribers(&event, cache);
                completed_seqs.push(seq);
            }
            Err(e) => {
                eprintln!("[OutboxWorker] Failed to parse event: {}", e);
                failed_seqs.push(seq);
            }
        }

        is_processed = true;
    }

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

    outbox_repo::mark_batch(&tx, &completed_seqs, outbox_repo::OutboxStatus::Completed)?;
    outbox_repo::mark_batch(&tx, &failed_seqs, outbox_repo::OutboxStatus::Failed)?;

    tx.commit()?;
    Ok(is_processed)
}
