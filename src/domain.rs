use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EventPayload {
    FlagCreated { key: String, is_enabled: bool },
    FlagToogled { key: String, is_enabled: bool },
    FlagArchived { key: String },
}

impl EventPayload {
    pub fn event_type(&self) -> &'static str {
        match self {
            EventPayload::FlagCreated { .. } => "FlagCreated",
            EventPayload::FlagToogled { .. } => "FlagToogled",
            EventPayload::FlagArchived { .. } => "FlagArchived",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: Uuid,
    pub flag_id: Uuid,
    pub actor_id: String,
    pub payload: EventPayload,
    pub occurred_at: i64,
}

#[derive(Debug, Clone)]
pub struct FeatureFlag {
    pub id: Uuid,
    pub key: String,
    pub is_enabled: bool,
    pub is_archived: bool,
    pub version: i64,
    pub updated_at: i64,
    pub domain_events: Vec<DomainEvent>,
}

impl FeatureFlag {
    pub fn new(key: String, actor_id: String) -> Self {
        let now = current_time_ms();
        let flag_id = Uuid::now_v7();

        let mut flag = Self {
            id: flag_id,
            key,
            is_enabled: false,
            is_archived: false,
            version: 1,
            updated_at: now,
            domain_events: Vec::new(),
        };

        flag.record_event(
            actor_id,
            EventPayload::FlagCreated {
                key: flag.key.clone(),
                is_enabled: flag.is_enabled,
            },
        );
        flag
    }

    pub fn toggle(&mut self, actor_id: String, new_state: bool) {
        if self.is_archived {
            return;
        }

        self.is_enabled = new_state;
        self.version += 1;
        self.updated_at = current_time_ms();

        self.record_event(
            actor_id,
            EventPayload::FlagToogled {
                key: self.key.clone(),
                is_enabled: new_state,
            },
        );
    }

    pub fn archive(&mut self, actor_id: String) {
        if self.is_archived {
            return;
        }

        self.is_archived = true;
        self.is_enabled = false;
        self.version += 1;
        self.updated_at = current_time_ms();

        self.record_event(
            actor_id,
            EventPayload::FlagArchived {
                key: self.key.clone(),
            },
        );
    }

    fn record_event(&mut self, actor_id: String, payload: EventPayload) {
        self.domain_events.push(DomainEvent {
            id: Uuid::now_v7(),
            flag_id: self.id,
            actor_id,
            payload,
            occurred_at: self.updated_at,
        });
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
