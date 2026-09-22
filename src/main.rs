mod db;
mod domain;
mod handlers;
mod repository;
mod service;
mod worker;
mod cache;

use std::{sync::Arc, time::Duration};

use db::init_db;
use std::thread;
use worker::OutboxWorker;

use crate::cache::FlagCache;

fn main() -> rusqlite::Result<()> {
    let db_path = "db.sqlite";

    init_db(db_path)?;

    let cache = Arc::new(FlagCache::new());
    OutboxWorker::new(db_path, cache, Duration::from_millis(500)).start()?;

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
