use arc_swap::ArcSwap;
use std::{collections::HashMap, sync::Arc};

use crate::domain::{DomainEvent, EventPayload};

pub struct FlagCache {
    flags: ArcSwap<HashMap<String, bool>>,
}

impl FlagCache {
    pub fn new() -> Self {
        Self {
            flags: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    pub fn is_enabled(&self, key: &str) -> bool {
        let guard = self.flags.load();
        guard.get(key).copied().unwrap_or(false)
    }

    pub fn apply(&self, events: &[DomainEvent]) {
        if events.is_empty() {
            return;
        }

        let mut map = HashMap::clone(&self.flags.load());
        for event in events {
            match &event.payload {
                EventPayload::FlagCreated { is_enabled }
                | EventPayload::FlagToggled { is_enabled } => {
                    map.insert(event.key.clone(), *is_enabled);
                }
                EventPayload::FlagArchived => {
                    map.remove(&event.key);
                }
            }
        }

        self.flags.store(Arc::new(map));
    }

    pub fn hydrate<I: IntoIterator<Item = (String, bool)>>(&self, flags: I) {
        let map = flags.into_iter().collect::<HashMap<String, bool>>();
        self.flags.store(Arc::new(map));
    }
}
