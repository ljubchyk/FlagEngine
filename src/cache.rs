use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

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
    pub fn update(&self, key: String, is_enabled: bool) {
        let mut current = (**self.flags.load()).clone();
        current.insert(key, is_enabled);
        self.flags.store(Arc::new(current));
    }
}
