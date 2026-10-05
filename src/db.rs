use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, Result};

pub type DbPool = Pool<SqliteConnectionManager>;

pub fn apply_parameters(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        // "PRAGMA foreign_keys = ON;
        "PRAGMA journal_mode = WAL;
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
        "CREATE TABLE IF NOT EXISTS feature_flags (
             key TEXT PRIMARY KEY,
             is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
             is_archived BOOLEAN NOT NULL DEFAULT FALSE,
             updated_at INTEGER NOT NULL
         ) WITHOUT ROWID;

         CREATE TABLE IF NOT EXISTS audit_logs (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             key TEXT NOT NULL,
             actor TEXT NOT NULL,
             action TEXT NOT NULL,
             payload TEXT NOT NULL,
             created_at INTEGER NOT NULL
         );

         CREATE INDEX IF NOT EXISTS idx_audit_logs_flag
         ON audit_logs(key, id);

         CREATE TABLE IF NOT EXISTS outbox_events (
             seq INTEGER PRIMARY KEY AUTOINCREMENT,
             status TEXT NOT NULL DEFAULT 'Pending' CHECK (status IN ('Pending', 'Completed', 'Failed')),
             payload TEXT NOT NULL,
             created_at INTEGER NOT NULL
         );

         CREATE INDEX IF NOT EXISTS idx_outbox_events_pending 
         ON outbox_events(seq) WHERE status = 'Pending';",
    )?;

    Ok(conn)
}
