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

use crate::cache::FlagCache;

fn main() -> rusqlite::Result<()> {
    let db_path = "db.sqlite";

    init_db(db_path)?;

    let cache = Arc::new(FlagCache::new());
    worker::spawn_outbox_worker(db_path, Duration::from_millis(500), cache)?;

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
