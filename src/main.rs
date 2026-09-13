mod db;
mod domain;
mod handlers;
mod repository;
mod service;
mod worker;

use std::time::Duration;

use db::init_db;
use std::thread;
use worker::OutboxWorker;

fn main() -> rusqlite::Result<()> {
    let db_path = "db.sqlite";
    init_db(db_path)?;

    OutboxWorker::new(db_path, Duration::from_millis(500)).start()?;

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
