use crate::domain::DomainEvent;
use rusqlite::{Result, Transaction, params};

pub fn handle_audit_log(tx: &Transaction, event: &DomainEvent) -> Result<()> {
    tx.execute(
        "INSERT INTO audit_logs (id, flag_id, actor_id, action, payload, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            event.id.to_string(),
            event.flag_id.to_string(),
            event.actor_id,
            event.event_type,
            serde_json::to_string(&event.payload).unwrap_or_default(),
            event.occurred_at
        ],
    )?;
    Ok(())
}

pub fn handle_outbox(tx: &Transaction, event: &DomainEvent) -> Result<()> {
    tx.execute(
        "INSERT INTO outbox_messages (id, event_type, payload, status, created_at)
         VALUES (?1, ?2, ?3, 'Pending', ?4)",
        params![
            event.id.to_string(),
            event.event_type,
            serde_json::to_string(event).unwrap_or_default(),
            event.occurred_at
        ],
    )?;
    Ok(())
}

pub fn process_events(tx: &Transaction, events: &[DomainEvent]) -> Result<()> {
    for event in events {
        handle_audit_log(tx, event)?;
        handle_outbox(tx, event)?;
    }
    Ok(())
}
