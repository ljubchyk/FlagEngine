use rusqlite::{Connection, Result};
use std::sync::{
    Arc,
    mpsc::{Receiver, RecvTimeoutError},
};
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
    wake_rx: Receiver<()>,
) -> Result<JoinHandle<()>> {
    let mut conn = Connection::open(&db_path)?;
    apply_parameters(&conn)?;

    let handle = thread::spawn(move || {
        loop {
            match process_pending_messages(&mut conn, &cache) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    eprintln!("[OutboxWorker] Error processing batch: {}", e);
                    thread::sleep(poll_interval);
                }
            }

            match wake_rx.recv_timeout(poll_interval) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break, // сервіс зник: зупиняємось
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

    let mut processed_count = 0;
    let mut events = Vec::with_capacity(rows.len());
    let mut completed_seqs = Vec::with_capacity(rows.len());
    let mut failed_seqs = Vec::new();

    for (seq, event) in rows {
        match event {
            Ok(event) => {
                events.push(event);
                completed_seqs.push(seq);
            }
            Err(e) => {
                eprintln!("[OutboxWorker] Failed to parse event: {}", e);
                failed_seqs.push(seq);
            }
        }

        processed_count += 1;
    }

    notify_async_subscribers(&events, cache);

    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    outbox_repo::mark_batch(&tx, &completed_seqs, outbox_repo::OutboxStatus::Completed)?;
    outbox_repo::mark_batch(&tx, &failed_seqs, outbox_repo::OutboxStatus::Failed)?;
    tx.commit()?;

    Ok(processed_count == 50)
}
