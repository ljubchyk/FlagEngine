use crate::domain::DomainEvent;
use crate::outbox_repo;
use crate::{audit_repo, cache::FlagCache};

use rusqlite::{Result, Transaction};

pub fn handle_audit_log(tx: &Transaction, event: &DomainEvent) -> Result<()> {
    audit_repo::append(tx, event)?;
    Ok(())
}

pub fn handle_outbox(tx: &Transaction, event: &DomainEvent) -> Result<()> {
    outbox_repo::enqueue(tx, event)?;
    Ok(())
}

pub fn notify_sync_subscribers(tx: &Transaction, event: &DomainEvent) -> Result<()> {
    handle_audit_log(tx, event)?;
    handle_outbox(tx, event)?;

    Ok(())
}

pub fn handle_dispatch(event: &DomainEvent) {
    println!("🚀 Dispatching event -> Payload: {:?}", event.payload);
}

pub fn handle_cache(event: &DomainEvent, cache: &FlagCache) {
    match &event.payload {
        crate::domain::EventPayload::FlagCreated { is_enabled }
        | crate::domain::EventPayload::FlagToggled { is_enabled } => {
            cache.update(&event.key, *is_enabled)
        }
        crate::domain::EventPayload::FlagArchived => cache.remove(&event.key),
    }
}

pub fn notify_async_subscribers(event: &DomainEvent, cache: &FlagCache) {
    handle_dispatch(event);
    handle_cache(event, cache);
}
