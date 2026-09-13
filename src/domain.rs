use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: Uuid,
    pub flag_id: Uuid,
    pub actor_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
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
    pub uncommitted_events: Vec<DomainEvent>,
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
            uncommitted_events: Vec::new(),
        };

        flag.uncommitted_events.push(DomainEvent {
            id: Uuid::now_v7(),
            flag_id,
            actor_id,
            event_type: "FlagCreated".to_string(),
            payload: serde_json::json!({ "is_enabled": false }),
            occurred_at: now,
        });

        flag
    }

    pub fn toggle(&mut self, actor_id: String, new_state: bool) {
        if self.is_archived {
            return;
        }

        self.is_enabled = new_state;
        self.version += 1;
        self.updated_at = current_time_ms();

        self.uncommitted_events.push(DomainEvent {
            id: Uuid::now_v7(),
            flag_id: self.id,
            actor_id,
            event_type: "FlagToggled".to_string(),
            payload: serde_json::json!({ "new_state": new_state }),
            occurred_at: self.updated_at,
        });
    }

    pub fn archive(&mut self, actor_id: String) {
        if self.is_archived {
            return;
        }

        self.is_archived = true;
        self.is_enabled = false;
        self.version += 1;
        self.updated_at = current_time_ms();

        self.uncommitted_events.push(DomainEvent {
            id: Uuid::now_v7(),
            flag_id: self.id,
            actor_id,
            event_type: "FlagArchived".to_string(),
            payload: serde_json::json!({ "key": self.key }),
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
