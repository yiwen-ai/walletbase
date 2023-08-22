use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Log {
    pub level: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
    pub graceful_shutdown: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ScyllaDB {
    pub nodes: Vec<String>,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Conf {
    pub env: String,
    pub log: Log,
    pub server: Server,
    pub scylla: ScyllaDB,
}

impl Conf {
    pub fn new() -> Result<Self, ConfigError> {
        let file_name =
            std::env::var("CONFIG_FILE_PATH").unwrap_or_else(|_| "./config/default.toml".into());
        Self::from(&file_name)
    }

    pub fn from(file_name: &str) -> Result<Self, ConfigError> {
        let builder = Config::builder().add_source(File::new(file_name, FileFormat::Toml));
        builder.build()?.try_deserialize::<Conf>()
    }
}
