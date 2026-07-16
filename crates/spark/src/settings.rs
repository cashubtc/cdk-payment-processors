use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Backend-specific configuration for Spark wallet
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// BIP39 mnemonic for wallet seed
    pub mnemonic: String,

    /// Data directory for persistent quote and Spark request mappings
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

fn default_data_dir() -> String {
    ".data/spark".to_string()
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            mnemonic: String::new(),
            data_dir: default_data_dir(),
        }
    }
}

/// Main configuration structure
///
/// Loads configuration from config.toml and environment variables.
/// Environment variables take precedence over file configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Backend type identifier (e.g., "spark")
    #[serde(default)]
    pub backend_type: String,

    /// Backend-specific configuration
    #[serde(default)]
    pub backend: BackendConfig,

    /// gRPC server address
    #[serde(default = "default_address")]
    pub address: String,

    /// gRPC server port
    #[serde(default = "default_port")]
    pub port: u16,

    /// TLS config for gRPC server
    pub tls_enable: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,
}

fn default_address() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    50051
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend_type: "spark".to_string(),
            backend: BackendConfig::default(),
            address: default_address(),
            port: default_port(),
            tls_enable: false,
            tls_cert_path: "certs/server.crt".to_string(),
            tls_key_path: "certs/server.key".to_string(),
        }
    }
}

impl Config {
    /// Load from config.toml (if present) and environment variables.
    /// Environment variables override file values.
    pub fn load() -> Self {
        // 1) Start with defaults + config.toml only if it exists
        let base: Config = Default::default();
        let mut fig = Figment::from(Serialized::defaults(base));
        if std::path::Path::new("config.toml").exists() {
            fig = fig.merge(Toml::file("config.toml"));
        }
        let mut cfg: Config = fig.extract().unwrap_or_default();

        // 2) Overlay environment variables explicitly
        if let Ok(v) = std::env::var("SPARK_MNEMONIC") {
            tracing::debug!("SPARK_MNEMONIC loaded from environment");
            cfg.backend.mnemonic = v;
        }
        if let Ok(v) = std::env::var("SPARK_DATA_DIR") {
            cfg.backend.data_dir = v;
        }
        // Server configuration
        if let Ok(v) = std::env::var("SERVER_ADDRESS") {
            cfg.address = v;
        }
        if let Ok(v) = std::env::var("SERVER_PORT") {
            cfg.port = v.parse().unwrap_or(cfg.port);
        }
        if let Ok(v) = std::env::var("TLS_ENABLE") {
            cfg.tls_enable = matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES");
        }
        if let Ok(v) = std::env::var("TLS_CERT_PATH") {
            cfg.tls_cert_path = v;
        }
        if let Ok(v) = std::env::var("TLS_KEY_PATH") {
            cfg.tls_key_path = v;
        }

        cfg
    }

    pub fn from_env() -> Self {
        Self::load()
    }
}
