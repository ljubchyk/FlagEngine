use std::{env, fmt::Display, net::SocketAddr, str::FromStr, time::Duration};

pub struct Config {
    pub addr: SocketAddr,
    pub db_path: String,
    pub admin_key: String,
    pub poll_interval: Duration,
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn parse_var<T>(name: &str, default: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    let raw = var(name).unwrap_or_else(|| default.to_owned());
    raw.parse()
        .map_err(|e| format!("{name} has invalid value '{raw}': {e}"))
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let admin_key = var("FLAGENGINE_ADMIN_KEY").ok_or("FLAGENGINE_ADMIN_KEY is required")?;
        let db_path = var("FLAGENGINE_DB_PATH").unwrap_or("db.sqlite".to_owned());
        let addr = parse_var::<SocketAddr>("FLAGENGINE_ADDR", "127.0.0.1:3000")?;
        let poll_ms = parse_var::<u64>("FLAGENGINE_POLL_INTERVAL_MS", "5000")?;

        Ok(Self {
            addr,
            db_path,
            admin_key,
            poll_interval: Duration::from_millis(poll_ms),
        })
    }
}
