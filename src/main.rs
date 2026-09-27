mod cache;
mod db;
mod domain;
mod handlers;
mod repository;
mod service;
mod worker;

use std::{sync::Arc, time::Duration};

use db::init_db;
use std::thread;

use crate::{cache::FlagCache, repository::SqliteFlagRepository};

fn main() -> rusqlite::Result<()> {
    let db_path = "db.sqlite";

    let conn = init_db(db_path)?;

    let cache = Arc::new(FlagCache::new());
    let repo = SqliteFlagRepository::new();
    let flags = repo.find_all_active(&conn)?;

    cache.hydrate(flags.into_iter().map(|flag| (flag.key, flag.is_enabled)));

    worker::spawn_outbox_worker(db_path, Duration::from_millis(500))?;

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
