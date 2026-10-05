use crate::domain::{DomainEvent, EventPayload};
use rusqlite::{
    Error, Result, ToSql, Transaction, params,
    types::ToSqlOutput,
    types::{FromSql, FromSqlError, FromSqlResult, ValueRef},
};

impl ToSql for EventPayload {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let json =
            serde_json::to_string(self).map_err(|e| Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(json))
    }
}

impl FromSql for EventPayload {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let text = value.as_str()?;
        serde_json::from_str(text).map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

fn action(payload: &EventPayload) -> &'static str {
    match payload {
        EventPayload::FlagCreated { .. } => "FlagCreated",
        EventPayload::FlagToggled { .. } => "FlagToggled",
        EventPayload::FlagArchived => "FlagArchived",
    }
}

pub fn append(tx: &Transaction, event: &DomainEvent) -> Result<()> {
    tx.execute(
        "INSERT INTO audit_logs (key, actor, action, payload, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event.key,
            event.actor,
            action(&event.payload),
            event.payload,
            event.occurred_at
        ],
    )?;
    Ok(())
}
