use crate::domain::Flag;
use rusqlite::{Connection, OptionalExtension, Result, Row, Transaction, params};

#[derive(Default)]
pub enum FlagFilter {
    #[default]
    Active,
    Archived,
    All,
}

fn raw_to_flag(row: &Row) -> Result<Flag> {
    Ok(Flag::restore(
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
    ))
}

pub fn insert(tx: &Transaction, flag: &Flag) -> Result<()> {
    tx.execute(
        "INSERT INTO flags (key, is_enabled, is_archived, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
        params![
            flag.key(),
            flag.is_enabled(),
            flag.is_archived(),
            flag.updated_at()
        ],
    )?;
    Ok(())
}

pub fn update(tx: &Transaction, flag: &Flag) -> Result<()> {
    tx.execute(
        "UPDATE flags SET is_enabled = ?2, is_archived = ?3, updated_at = ?4 WHERE key = ?1",
        params![
            flag.key(),
            flag.is_enabled(),
            flag.is_archived(),
            flag.updated_at()
        ],
    )?;
    Ok(())
}

pub fn find(conn: &Connection, key: &str) -> Result<Option<Flag>> {
    conn.prepare_cached(
        "SELECT key, is_enabled, is_archived, updated_at FROM flags WHERE key = ?1",
    )?
    .query_row(params![key], raw_to_flag)
    .optional()
}

pub fn find_all(conn: &Connection, filter: FlagFilter) -> Result<Vec<Flag>> {
    let where_closure = match filter {
        FlagFilter::Active => " WHERE is_archived = 0",
        FlagFilter::Archived => " WHERE is_archived = 1",
        FlagFilter::All => "",
    };
    let sql = format!(
        "SELECT key, is_enabled, is_archived, updated_at FROM flags{}",
        where_closure
    );
    conn.prepare_cached(&sql)?
        .query_map([], raw_to_flag)?
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn setup() -> Connection {
        db::init(":memory:").unwrap()
    }

    fn flag(key: &str) -> Flag {
        Flag::create(key.to_owned(), "test".to_owned()).unwrap().0
    }

    fn save(conn: &mut Connection, f: &Flag) {
        let tx = conn.transaction().unwrap();
        insert(&tx, f).unwrap();
        tx.commit().unwrap();
    }

    fn persist_update(conn: &mut Connection, f: &Flag) {
        let tx = conn.transaction().unwrap();
        update(&tx, f).unwrap();
        tx.commit().unwrap();
    }

    fn keys(flags: &[Flag]) -> Vec<&str> {
        let mut k: Vec<&str> = flags.iter().map(|f| f.key()).collect();
        k.sort_unstable();
        k
    }

    // ---------- insert / find ----------

    #[test]
    fn insert_then_find_roundtrip() {
        let mut conn = setup();
        let f = flag("checkout");
        save(&mut conn, &f);

        let found = find(&conn, "checkout").unwrap().expect("flag must exist");
        assert_eq!(found.key(), "checkout");
        assert!(!found.is_enabled());
        assert!(!found.is_archived());
        assert_eq!(found.updated_at(), f.updated_at());
    }

    #[test]
    fn find_missing_returns_none() {
        let conn = setup();
        assert!(find(&conn, "nope").unwrap().is_none());
    }

    #[test]
    fn find_is_exact_match_not_prefix() {
        let mut conn = setup();
        save(&mut conn, &flag("checkout"));
        assert!(find(&conn, "check").unwrap().is_none());
        assert!(find(&conn, "checkout.v2").unwrap().is_none());
    }

    #[test]
    fn insert_duplicate_key_fails_and_keeps_original() {
        let mut conn = setup();
        let original = flag("checkout");
        save(&mut conn, &original);

        let tx = conn.transaction().unwrap();
        assert!(insert(&tx, &flag("checkout")).is_err());
        drop(tx);

        let found = find(&conn, "checkout").unwrap().unwrap();
        assert_eq!(found.updated_at(), original.updated_at());
    }

    #[test]
    fn insert_is_not_visible_after_rollback() {
        let mut conn = setup();
        let tx = conn.transaction().unwrap();
        insert(&tx, &flag("checkout")).unwrap();
        tx.rollback().unwrap();

        assert!(find(&conn, "checkout").unwrap().is_none());
    }

    #[test]
    fn insert_preserves_enabled_and_archived_state() {
        let mut conn = setup();
        let enabled = Flag::restore("on".into(), true, false, 10);
        let archived = Flag::restore("old".into(), false, true, 20);
        save(&mut conn, &enabled);
        save(&mut conn, &archived);

        let on = find(&conn, "on").unwrap().unwrap();
        assert!(on.is_enabled() && !on.is_archived());
        assert_eq!(on.updated_at(), 10);

        let old = find(&conn, "old").unwrap().unwrap();
        assert!(!old.is_enabled() && old.is_archived());
        assert_eq!(old.updated_at(), 20);
    }

    // ---------- update ----------

    #[test]
    fn update_persists_toggle() {
        let mut conn = setup();
        let mut f = flag("checkout");
        save(&mut conn, &f);

        f.set_enabled(true, "alice".into()).unwrap().unwrap();
        persist_update(&mut conn, &f);

        let found = find(&conn, "checkout").unwrap().unwrap();
        assert!(found.is_enabled());
        assert_eq!(found.updated_at(), f.updated_at());
    }

    #[test]
    fn update_persists_archive_and_forces_disabled() {
        let mut conn = setup();
        let mut f = flag("checkout");
        f.set_enabled(true, "alice".into()).unwrap();
        save(&mut conn, &f);

        f.archive("alice".into()).unwrap();
        persist_update(&mut conn, &f);

        let found = find(&conn, "checkout").unwrap().unwrap();
        assert!(found.is_archived());
        assert!(!found.is_enabled());
    }

    /// Регресійний тест: `update` повинен зачіпати лише рядок із потрібним key.
    #[test]
    fn update_affects_only_the_target_flag() {
        let mut conn = setup();
        let mut a = flag("a");
        let b = Flag::restore("b".into(), false, false, 1);
        let c = Flag::restore("c".into(), true, false, 2);
        save(&mut conn, &a);
        save(&mut conn, &b);
        save(&mut conn, &c);

        a.set_enabled(true, "alice".into()).unwrap().unwrap();
        persist_update(&mut conn, &a);

        let b_after = find(&conn, "b").unwrap().unwrap();
        assert!(!b_after.is_enabled(), "b must stay disabled");
        assert_eq!(b_after.updated_at(), 1, "b.updated_at must not change");

        let c_after = find(&conn, "c").unwrap().unwrap();
        assert!(c_after.is_enabled(), "c must stay enabled");
        assert_eq!(c_after.updated_at(), 2, "c.updated_at must not change");
    }

    #[test]
    fn update_of_missing_key_is_noop() {
        let mut conn = setup();
        save(&mut conn, &flag("existing"));

        let ghost = Flag::restore("ghost".into(), true, false, 99);
        persist_update(&mut conn, &ghost);

        assert!(find(&conn, "ghost").unwrap().is_none());
        let existing = find(&conn, "existing").unwrap().unwrap();
        assert!(!existing.is_enabled());
    }

    #[test]
    fn update_is_not_visible_after_rollback() {
        let mut conn = setup();
        let mut f = flag("checkout");
        save(&mut conn, &f);

        f.set_enabled(true, "alice".into()).unwrap();
        let tx = conn.transaction().unwrap();
        update(&tx, &f).unwrap();
        tx.rollback().unwrap();

        assert!(!find(&conn, "checkout").unwrap().unwrap().is_enabled());
    }

    // ---------- find_all ----------

    fn seed_mixed(conn: &mut Connection) {
        save(conn, &Flag::restore("active-off".into(), false, false, 1));
        save(conn, &Flag::restore("active-on".into(), true, false, 2));
        save(conn, &Flag::restore("archived".into(), false, true, 3));
    }

    #[test]
    fn find_all_on_empty_table_is_empty() {
        let conn = setup();
        for filter in [FlagFilter::Active, FlagFilter::Archived, FlagFilter::All] {
            assert!(find_all(&conn, filter).unwrap().is_empty());
        }
    }

    #[test]
    fn find_all_default_filter_is_active() {
        let mut conn = setup();
        seed_mixed(&mut conn);

        let flags = find_all(&conn, FlagFilter::default()).unwrap();
        assert_eq!(keys(&flags), ["active-off", "active-on"]);
    }

    #[test]
    fn find_all_active_excludes_archived() {
        let mut conn = setup();
        seed_mixed(&mut conn);

        let flags = find_all(&conn, FlagFilter::Active).unwrap();
        assert_eq!(keys(&flags), ["active-off", "active-on"]);
        assert!(flags.iter().all(|f| !f.is_archived()));
    }

    #[test]
    fn find_all_archived_returns_only_archived() {
        let mut conn = setup();
        seed_mixed(&mut conn);

        let flags = find_all(&conn, FlagFilter::Archived).unwrap();
        assert_eq!(keys(&flags), ["archived"]);
    }

    #[test]
    fn find_all_all_returns_everything() {
        let mut conn = setup();
        seed_mixed(&mut conn);

        let flags = find_all(&conn, FlagFilter::All).unwrap();
        assert_eq!(keys(&flags), ["active-off", "active-on", "archived"]);
    }

    #[test]
    fn find_all_reflects_archive_via_update() {
        let mut conn = setup();
        let mut f = flag("checkout");
        save(&mut conn, &f);
        assert_eq!(find_all(&conn, FlagFilter::Active).unwrap().len(), 1);

        f.archive("alice".into()).unwrap();
        persist_update(&mut conn, &f);

        assert!(find_all(&conn, FlagFilter::Active).unwrap().is_empty());
        assert_eq!(find_all(&conn, FlagFilter::Archived).unwrap().len(), 1);
    }

    #[test]
    fn find_all_can_be_called_repeatedly_with_cached_statements() {
        let mut conn = setup();
        seed_mixed(&mut conn);

        for _ in 0..3 {
            assert_eq!(find_all(&conn, FlagFilter::Active).unwrap().len(), 2);
            assert_eq!(find_all(&conn, FlagFilter::All).unwrap().len(), 3);
        }
    }
}
