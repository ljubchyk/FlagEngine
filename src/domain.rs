use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EventPayload {
    FlagCreated { is_enabled: bool },
    FlagEnabled,
    FlagDisabled,
    FlagArchived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub key: String,
    pub actor: String,
    pub payload: EventPayload,
    pub occurred_at: i64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970")
        .as_millis() as i64
}

fn validate_key(raw: &str) -> Result<(), AppError> {
    if raw.is_empty() || raw.len() > 64 {
        return Err(AppError::InvalidKey("must be 1-64 characters"));
    }

    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    if !raw
        .chars()
        .all(|c| alnum(c) || matches!(c, '.' | '_' | '-'))
    {
        return Err(AppError::InvalidKey(
            "only a-z, 0-9, '.', '_' and '-' are allowed",
        ));
    }
    if !raw.starts_with(alnum) || !raw.ends_with(alnum) {
        return Err(AppError::InvalidKey(
            "must start and end with a letter or digit",
        ));
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct Flag {
    key: String,
    is_enabled: bool,
    is_archived: bool,
    updated_at: i64,
}

impl Flag {
    pub fn create(key: String, actor: String) -> Result<(Self, DomainEvent), AppError> {
        validate_key(&key)?;

        let flag = Self {
            key,
            is_enabled: false,
            is_archived: false,
            updated_at: now_ms(),
        };

        let event = flag.event(
            actor,
            EventPayload::FlagCreated {
                is_enabled: flag.is_enabled,
            },
        );

        Ok((flag, event))
    }

    pub fn enable(&mut self, actor: String) -> Result<Option<DomainEvent>, AppError> {
        if self.is_archived {
            return Err(AppError::Archived);
        }

        if self.is_enabled {
            return Ok(None);
        }

        self.is_enabled = true;
        self.updated_at = now_ms();

        let event = self.event(actor, EventPayload::FlagEnabled);

        Ok(Some(event))
    }

    pub fn disable(&mut self, actor: String) -> Result<Option<DomainEvent>, AppError> {
        if self.is_archived {
            return Err(AppError::Archived);
        }

        if !self.is_enabled {
            return Ok(None);
        }

        self.is_enabled = false;
        self.updated_at = now_ms();

        let event = self.event(actor, EventPayload::FlagDisabled);

        Ok(Some(event))
    }

    pub fn archive(&mut self, actor: String) -> Option<DomainEvent> {
        if self.is_archived {
            return None;
        }

        self.is_archived = true;
        self.is_enabled = false;
        self.updated_at = now_ms();

        let event = self.event(actor, EventPayload::FlagArchived);

        Some(event)
    }

    pub(crate) fn restore(key: String, enabled: bool, archived: bool, updated_at: i64) -> Self {
        Self {
            key,
            is_enabled: enabled,
            is_archived: archived,
            updated_at,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn is_enabled(&self) -> bool {
        self.is_enabled
    }

    pub fn is_archived(&self) -> bool {
        self.is_archived
    }

    pub fn updated_at(&self) -> i64 {
        self.updated_at
    }

    fn event(&self, actor: String, payload: EventPayload) -> DomainEvent {
        DomainEvent {
            key: self.key.to_owned(),
            actor,
            payload,
            occurred_at: self.updated_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_invalid_keys() {
        for bad in [
            "",
            "Upper",
            "has space",
            "-lead",
            "trail-",
            "ключ",
            &"a".repeat(65),
        ] {
            assert!(validate_key(bad).is_err(), "{bad:?} must be reected");
        }

        assert!(validate_key("new-checkout.v2").is_ok());
    }

    #[test]
    fn create_returns_flag_and_event() {
        let (flag, event) = Flag::create("checkout".to_owned(), "alice".to_owned()).unwrap();
        assert_eq!(flag.key(), "checkout");
        assert_eq!(event.key, "checkout");
        assert_eq!(event.actor, "alice");
        assert_eq!(event.occurred_at, flag.updated_at());
        assert!(matches!(
            event.payload,
            EventPayload::FlagCreated { is_enabled: false }
        ))
    }

    #[test]
    fn create_with_invalid_key_fails() {
        assert!(Flag::create("Bad key".to_owned(), "alice".to_owned()).is_err());
    }

    #[test]
    fn set_enabled_is_idempotent() {
        let (mut f, _) = Flag::create("checkout".to_owned(), "alice".to_owned()).unwrap();
        assert!(f.disable("alice".to_owned()).unwrap().is_none());

        let event = f
            .enable("alice".to_owned())
            .unwrap()
            .expect("state changed");
        assert!(matches!(event.payload, EventPayload::FlagEnabled));
        assert!(f.is_enabled());
        assert_eq!(event.occurred_at, f.updated_at());
    }

    #[test]
    fn archived_flag_cannot_change() {
        let (mut f, _) = Flag::create("checkout".to_owned(), "alice".to_owned()).unwrap();
        assert!(f.archive("alice".to_owned()).is_some());
        assert!(f.archive("alice".to_owned()).is_none());
        assert_eq!(
            f.enable("alice".to_owned()).unwrap_err(),
            DomainError::Archived
        );
    }

    #[test]
    fn event_json_format_is_stable() {
        let json = r#"{"key":"checkout","actor":"alice","occurred_at":1,
                       "payload":{"type":"FlagEnabled"}}"#;
        let e: DomainEvent = serde_json::from_str(json).unwrap();
        assert_eq!(e.key, "checkout");
        assert!(matches!(e.payload, EventPayload::FlagEnabled));
    }
}
