use std::sync::Arc;

use crate::cache::FlagCache;
use crate::handlers::notify_sync_subscribers;
use crate::{db::DbPool, domain::Flag};
use crate::{domain, flag_repo};
use rusqlite::TransactionBehavior;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("Feature flag with key '{0}' was not found")]
    FlagNotFound(String),

    #[error("Feature flag with key '{0}' already exists")]
    DuplicateKey(String),

    #[error("Database operation failed: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Failed to acquire connection from pool: {0}")]
    Pool(#[from] r2d2::Error),

    #[error("...")]
    Domain(#[from] domain::DomainError),
}

pub type Result<T> = std::result::Result<T, ServiceError>;

fn is_unique_violation(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(inner, _) => {
            inner.code == rusqlite::ErrorCode::ConstraintViolation && inner.extended_code == 1555
        }
        _ => false,
    }
}

pub struct FlagService {
    pool: DbPool,
    cache: Arc<FlagCache>,
}

impl FlagService {
    pub fn new(pool: DbPool, cache: Arc<FlagCache>) -> Self {
        Self { pool, cache }
    }

    pub fn is_enabled(&self, key: &str) -> bool {
        self.cache.is_enabled(key)
    }

    pub fn create_flag(&self, key: String, actor: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let (flag, event) = Flag::create(key.clone(), actor)?;

        flag_repo::insert(&tx, &flag).map_err(|err| {
            if is_unique_violation(&err) {
                ServiceError::DuplicateKey(key)
            } else {
                ServiceError::Database(err)
            }
        })?;
        notify_sync_subscribers(&tx, &event)?;

        tx.commit()?;
        Ok(())
    }

    pub fn toggle_flag(&self, key: String, enabled: bool, actor: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut flag = flag_repo::find(&tx, &key)?.ok_or(ServiceError::FlagNotFound(key))?;

        if let Some(event) = flag.set_enabled(enabled, actor)? {
            flag_repo::update(&tx, &flag)?;
            notify_sync_subscribers(&tx, &event)?;

            tx.commit()?;
        }

        Ok(())
    }

    pub fn archive_flag(&self, key: String, actor: String) -> Result<()> {
        let mut conn = self.pool.get()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut flag = flag_repo::find(&tx, &key)?.ok_or(ServiceError::FlagNotFound(key))?;

        if let Some(event) = flag.archive(actor) {
            flag_repo::update(&tx, &flag)?;
            notify_sync_subscribers(&tx, &event)?;

            tx.commit()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DomainError;
    use crate::{db, handlers, outbox_repo};
    use std::sync::Barrier;
    use std::thread;
    use tempfile::TempDir;

    struct Env {
        svc: FlagService,
        pool: DbPool,
        cache: Arc<FlagCache>,
        _dir: TempDir, // тримаємо, щоб файл БД жив до кінця тесту
    }

    // Пул потребує файлової БД: кожне з'єднання `:memory:` було б окремою базою.
    fn setup() -> Env {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sqlite");
        let path = path.to_str().unwrap();

        db::init(path).unwrap();
        let pool = db::create_pool(path).unwrap();
        let cache = Arc::new(FlagCache::new());
        let svc = FlagService::new(pool.clone(), cache.clone());

        Env { svc, pool, cache, _dir: dir }
    }

    impl Env {
        fn count(&self, table: &str) -> i64 {
            self.pool
                .get()
                .unwrap()
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                .unwrap()
        }

        fn flag(&self, key: &str) -> Option<Flag> {
            flag_repo::find(&self.pool.get().unwrap(), key).unwrap()
        }

        /// Емуляція одного проходу outbox-воркера: застосувати події до кешу.
        fn apply_outbox_to_cache(&self) {
            let conn = self.pool.get().unwrap();
            for (_, event) in outbox_repo::fetch_pending(&conn, 100).unwrap() {
                handlers::notify_async_subscribers(&event.unwrap(), &self.cache);
            }
        }

        fn create(&self, key: &str) {
            self.svc.create_flag(key.into(), "alice".into()).unwrap();
        }
    }

    // ---------- create_flag ----------

    #[test]
    fn create_persists_flag_disabled_and_not_archived() {
        let env = setup();
        env.create("checkout");

        let f = env.flag("checkout").expect("flag must exist");
        assert!(!f.is_enabled());
        assert!(!f.is_archived());
    }

    #[test]
    fn create_writes_one_audit_row_and_one_outbox_event_atomically() {
        let env = setup();
        env.create("checkout");

        assert_eq!(env.count("flags"), 1);
        assert_eq!(env.count("audit"), 1);
        assert_eq!(env.count("outbox"), 1);
    }

    #[test]
    fn create_records_actor_in_audit_log() {
        let env = setup();
        env.svc.create_flag("checkout".into(), "bob".into()).unwrap();

        let (actor, action): (String, String) = env
            .pool
            .get()
            .unwrap()
            .query_row("SELECT actor, action FROM audit", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(actor, "bob");
        assert_eq!(action, "FlagCreated");
    }

    #[test]
    fn create_duplicate_returns_duplicate_key_and_writes_nothing_more() {
        let env = setup();
        env.create("checkout");

        let err = env
            .svc
            .create_flag("checkout".into(), "alice".into())
            .unwrap_err();
        assert!(matches!(err, ServiceError::DuplicateKey(ref k) if k == "checkout"));

        assert_eq!(env.count("flags"), 1);
        assert_eq!(env.count("audit"), 1);
        assert_eq!(env.count("outbox"), 1);
    }

    #[test]
    fn create_with_invalid_key_returns_domain_error_and_writes_nothing() {
        let env = setup();
        let err = env
            .svc
            .create_flag("Bad Key".into(), "alice".into())
            .unwrap_err();
        assert!(matches!(err, ServiceError::Domain(DomainError::InvalidKey(_))));

        assert_eq!(env.count("flags"), 0);
        assert_eq!(env.count("audit"), 0);
        assert_eq!(env.count("outbox"), 0);
    }

    #[test]
    fn create_rolls_back_flag_when_audit_write_fails() {
        let env = setup();
        env.pool
            .get()
            .unwrap()
            .execute_batch("DROP TABLE audit")
            .unwrap();

        let err = env
            .svc
            .create_flag("checkout".into(), "alice".into())
            .unwrap_err();
        assert!(matches!(err, ServiceError::Database(_)));

        assert!(env.flag("checkout").is_none(), "flag insert must be rolled back");
        assert_eq!(env.count("outbox"), 0);
    }

    #[test]
    fn concurrent_creates_of_same_key_yield_exactly_one_success() {
        let env = setup();
        let svc = Arc::new(env.svc);
        let n = 8;
        let barrier = Arc::new(Barrier::new(n));

        let handles: Vec<_> = (0..n)
            .map(|i| {
                let svc = svc.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    svc.create_flag("checkout".into(), format!("user-{i}"))
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let ok = results.iter().filter(|r| r.is_ok()).count();
        let dup = results
            .iter()
            .filter(|r| matches!(r, Err(ServiceError::DuplicateKey(_))))
            .count();

        assert_eq!(ok, 1);
        assert_eq!(dup, n - 1, "all others must be DuplicateKey: {results:?}");
        let conn = env.pool.get().unwrap();
        let audit: i64 = conn
            .query_row("SELECT COUNT(*) FROM audit", [], |r| r.get(0))
            .unwrap();
        assert_eq!(audit, 1);
    }

    // ---------- toggle_flag ----------

    #[test]
    fn toggle_unknown_flag_returns_not_found() {
        let env = setup();
        let err = env
            .svc
            .toggle_flag("ghost".into(), true, "alice".into())
            .unwrap_err();
        assert!(matches!(err, ServiceError::FlagNotFound(ref k) if k == "ghost"));
        assert_eq!(env.count("audit"), 0);
    }

    #[test]
    fn toggle_enables_flag_and_emits_audit_and_outbox() {
        let env = setup();
        env.create("checkout");
        env.svc
            .toggle_flag("checkout".into(), true, "alice".into())
            .unwrap();

        assert!(env.flag("checkout").unwrap().is_enabled());
        assert_eq!(env.count("audit"), 2); // created + toggled
        assert_eq!(env.count("outbox"), 2);
    }

    #[test]
    fn toggle_to_same_state_is_noop_without_events() {
        let env = setup();
        env.create("checkout");
        let before = env.flag("checkout").unwrap().updated_at();

        env.svc
            .toggle_flag("checkout".into(), false, "alice".into())
            .unwrap();

        assert_eq!(env.count("audit"), 1);
        assert_eq!(env.count("outbox"), 1);
        assert_eq!(env.flag("checkout").unwrap().updated_at(), before);
    }

    #[test]
    fn toggle_twice_to_same_value_emits_single_event() {
        let env = setup();
        env.create("checkout");
        for _ in 0..2 {
            env.svc
                .toggle_flag("checkout".into(), true, "alice".into())
                .unwrap();
        }
        assert_eq!(env.count("audit"), 2);
    }

    #[test]
    fn toggle_archived_flag_fails_with_archived_error() {
        let env = setup();
        env.create("checkout");
        env.svc.archive_flag("checkout".into(), "alice".into()).unwrap();

        let err = env
            .svc
            .toggle_flag("checkout".into(), true, "alice".into())
            .unwrap_err();
        assert!(matches!(err, ServiceError::Domain(DomainError::Archived)));
        assert!(!env.flag("checkout").unwrap().is_enabled());
    }

    #[test]
    fn toggle_does_not_touch_other_flags() {
        let env = setup();
        env.create("a");
        env.create("b");
        env.svc.toggle_flag("a".into(), true, "alice".into()).unwrap();

        assert!(env.flag("a").unwrap().is_enabled());
        assert!(!env.flag("b").unwrap().is_enabled());
    }

    // ---------- archive_flag ----------

    #[test]
    fn archive_unknown_flag_returns_not_found() {
        let env = setup();
        let err = env
            .svc
            .archive_flag("ghost".into(), "alice".into())
            .unwrap_err();
        assert!(matches!(err, ServiceError::FlagNotFound(_)));
    }

    #[test]
    fn archive_marks_flag_archived_and_disables_it() {
        let env = setup();
        env.create("checkout");
        env.svc
            .toggle_flag("checkout".into(), true, "alice".into())
            .unwrap();

        env.svc.archive_flag("checkout".into(), "alice".into()).unwrap();

        let f = env.flag("checkout").unwrap();
        assert!(f.is_archived());
        assert!(!f.is_enabled());
        assert_eq!(env.count("audit"), 3); // created, toggled, archived
    }

    #[test]
    fn archive_twice_is_idempotent() {
        let env = setup();
        env.create("checkout");
        env.svc.archive_flag("checkout".into(), "alice".into()).unwrap();
        env.svc.archive_flag("checkout".into(), "alice".into()).unwrap();

        assert_eq!(env.count("audit"), 2); // created + archived
        assert_eq!(env.count("outbox"), 2);
    }

    #[test]
    fn archive_does_not_touch_other_flags() {
        let env = setup();
        env.create("a");
        env.create("b");
        env.svc.toggle_flag("b".into(), true, "alice".into()).unwrap();

        env.svc.archive_flag("a".into(), "alice".into()).unwrap();

        let b = env.flag("b").unwrap();
        assert!(!b.is_archived());
        assert!(b.is_enabled());
    }

    // ---------- is_enabled / cache (eventual consistency) ----------

    #[test]
    fn is_enabled_is_false_for_unknown_flag() {
        let env = setup();
        assert!(!env.svc.is_enabled("ghost"));
    }

    #[test]
    fn cache_is_updated_only_after_outbox_is_processed() {
        let env = setup();
        env.create("checkout");
        env.svc
            .toggle_flag("checkout".into(), true, "alice".into())
            .unwrap();

        assert!(
            !env.svc.is_enabled("checkout"),
            "cache must not change before the outbox worker runs"
        );

        env.apply_outbox_to_cache();
        assert!(env.svc.is_enabled("checkout"));
    }

    #[test]
    fn archived_flag_disappears_from_cache_after_outbox_is_processed() {
        let env = setup();
        env.create("checkout");
        env.svc
            .toggle_flag("checkout".into(), true, "alice".into())
            .unwrap();
        env.apply_outbox_to_cache();
        assert!(env.svc.is_enabled("checkout"));

        env.svc.archive_flag("checkout".into(), "alice".into()).unwrap();
        env.apply_outbox_to_cache();
        assert!(!env.svc.is_enabled("checkout"));
    }

    // ---------- is_unique_violation ----------

    #[test]
    fn is_unique_violation_detects_primary_key_conflict_only() {
        let conn = db::init(":memory:").unwrap();
        conn.execute("INSERT INTO flags VALUES ('k', 0, 0, 1)", [])
            .unwrap();
        let dup = conn
            .execute("INSERT INTO flags VALUES ('k', 0, 0, 1)", [])
            .unwrap_err();
        assert!(is_unique_violation(&dup));

        let not_null = conn
            .execute("INSERT INTO flags (key, updated_at) VALUES ('x', NULL)", [])
            .unwrap_err();
        assert!(!is_unique_violation(&not_null));
        assert!(!is_unique_violation(&rusqlite::Error::QueryReturnedNoRows));
    }
}
