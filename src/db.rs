use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, Result};

pub type DbPool = Pool<SqliteConnectionManager>;

pub fn apply_parameters(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;",
    )
}

pub fn create_pool(db_path: &str) -> Result<DbPool, r2d2::Error> {
    let manager = SqliteConnectionManager::file(db_path).with_init(|conn| apply_parameters(conn));
    Pool::builder().max_size(10).build(manager)
}

pub fn init_db(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;
         PRAGMA foreign_keys = ON;

         CREATE TABLE IF NOT EXISTS feature_flags (
             id TEXT PRIMARY KEY,
             key TEXT NOT NULL UNIQUE,
             is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
             is_archived BOOLEAN NOT NULL DEFAULT FALSE,
             updated_at INTEGER NOT NULL
         );

         CREATE TABLE IF NOT EXISTS audit_logs (
             id TEXT PRIMARY KEY,
             flag_id TEXT NOT NULL,
             actor_id TEXT NOT NULL,
             action TEXT NOT NULL,
             payload TEXT NOT NULL,
             created_at INTEGER NOT NULL
         );

         CREATE TABLE IF NOT EXISTS outbox_messages (
             id TEXT PRIMARY KEY,
             event_type TEXT NOT NULL,
             payload TEXT NOT NULL,
             status TEXT NOT NULL CHECK (status IN ('Pending', 'Completed', 'Failed')),
             created_at INTEGER NOT NULL
         );

         CREATE INDEX IF NOT EXISTS idx_outbox_pending 
         ON outbox_messages(created_at) WHERE status = 'Pending';",
    )?;

    Ok(conn)
}
