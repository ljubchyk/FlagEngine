use std::{env, net::SocketAddr, time::Duration};

pub struct Config {
    pub addr: SocketAddr,
    pub db_path: String,
    pub admin_key: String,
    pub poll_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0} is required")]
    Missing(&'static str),

    #[error("{name} has invalid value '{value}': {reason}")]
    Invalid {
        name: &'static str,
        value: String,
        reason: String,
    },
}

/// Порожня змінна вважається незаданою.
fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let admin_key =
            var("FLAGENGINE_ADMIN_KEY").ok_or(ConfigError::Missing("FLAGENGINE_ADMIN_KEY"))?;

        let db_path = var("FLAGENGINE_DB_PATH").unwrap_or_else(|| "db.sqlite".to_owned());

        let addr_raw = var("FLAGENGINE_ADDR").unwrap_or_else(|| "127.0.0.1:3000".to_owned());
        let addr = addr_raw
            .parse::<SocketAddr>()
            .map_err(|e| ConfigError::Invalid {
                name: "FLAGENGINE_ADDR",
                value: addr_raw,
                reason: e.to_string(),
            })?;

        let poll_raw = var("FLAGENGINE_POLL_INTERVAL_MS").unwrap_or_else(|| "5000".to_owned());
        let poll_ms = poll_raw.parse::<u64>().map_err(|e| ConfigError::Invalid {
            name: "FLAGENGINE_POLL_INTERVAL_MS",
            value: poll_raw,
            reason: e.to_string(),
        })?;

        Ok(Self {
            addr,
            db_path,
            admin_key,
            poll_interval: Duration::from_millis(poll_ms),
        })
    }
}
