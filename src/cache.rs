use arc_swap::ArcSwap;
use std::{collections::HashMap, sync::Arc};

pub struct FlagCache {
    // Атомарний вказівник на таблицю прапорців (Lock-Free reads)
    flags: ArcSwap<HashMap<String, bool>>,
}

impl FlagCache {
    pub fn new() -> Self {
        Self {
            flags: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Зчитування стану прапорця за O(1) без захоплення Mutex/RwLock
    pub fn is_enabled(&self, key: &str) -> bool {
        let guard = self.flags.load();
        guard.get(key).copied().unwrap_or(false)
    }

    /// Атомарне оновлення кешу при отриманні події з Outbox
    pub fn update(&self, key: &str, is_enabled: bool) {
        self.flags.rcu(|current| {
            let mut new_map = (**current).clone();
            new_map.insert(key.into(), is_enabled);
            new_map
        });
    }

    pub fn remove(&self, key: &str) {
        self.flags.rcu(|map| {
            let mut new_map = (**map).clone();
            new_map.remove(key);
            new_map
        });
    }

    pub fn hydrate<I: IntoIterator<Item = (String, bool)>>(&self, flags: I) {
        let map = flags.into_iter().collect::<HashMap<String, bool>>();
        self.flags.store(Arc::new(map));
    }
}
