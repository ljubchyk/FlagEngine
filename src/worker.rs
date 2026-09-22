use rusqlite::{Connection, Result, params};
use std::{sync::Arc, thread, time::Duration};

use crate::{cache::FlagCache, db::apply_parameters};

pub struct OutboxWorker {
    db_path: String,
    cache: Arc<FlagCache>,
    poll_interval: Duration,
}

impl OutboxWorker {
    pub fn new(db_path: &str, cache: Arc<FlagCache>, poll_interval: Duration) -> Self {
        Self {
            db_path: db_path.to_string(),
            cache,
            poll_interval,
        }
    }

    /// Запуск воркера в окремому фоновому потоці
    pub fn start(self) -> Result<()> {
        let mut conn = Connection::open(&self.db_path)?;
        apply_parameters(&conn)?;

        let cache = self.cache.clone();
        thread::spawn(move || {
            loop {
                if let Err(e) = Self::process_pending_messages(&mut conn, &cache) {
                    eprintln!("[OutboxWorker] Error processing batch: {}", e);
                }

                // Засинаємо, якщо немає нових подій, щоб не навантажувати CPU
                thread::sleep(self.poll_interval);
            }
        });

        Ok(())
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
        let mut stmt =
            tx.prepare_cached("UPDATE outbox_messages SET status = 'Completed' WHERE id = ?1")?;

        let mut processed_count = 0;

        for (id, event_type, payload) in messages {
            // --- ІМІТАЦІЯ ВІДПРАВКИ ---
            // Тут у майбутньому буде виклик вебхука, стріму в SDK чи експорт у Kafka клієнта
            println!(
                "🚀 [OutboxWorker] Dispatching event -> Type: {}, Payload: {}",
                event_type, payload
            );

            // Позначаємо подію як успішно оброблену
            stmt.execute(params![id])?;

            processed_count += 1;
        }

        drop(stmt);

        tx.commit()?;
        Ok(processed_count)
    }
}
