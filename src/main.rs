mod audit_repo;
mod cache;
mod db;
mod domain;
mod flag_repo;
mod handlers;
mod outbox_repo;
mod service;
mod worker;

use std::{sync::Arc, time::Duration};

use db::init;
use std::thread;

use crate::{cache::FlagCache, flag_repo::FlagFilter};

fn main() -> rusqlite::Result<()> {
    let db_path = "db.sqlite";

    let conn = init(db_path)?;

    let cache = Arc::new(FlagCache::new());
    let flags = flag_repo::find_all(&conn, FlagFilter::Active)?;

    cache.hydrate(
        flags
            .into_iter()
            .map(|flag| (flag.key().to_owned(), flag.is_enabled())),
    );

    worker::spawn_outbox_worker(db_path, Duration::from_millis(500), cache)?;

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
