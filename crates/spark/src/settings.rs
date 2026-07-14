use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Backend-specific configuration for Spark wallet
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// Breez API key (required)
    pub api_key: String,

    /// Mnemonic seed phrase for the wallet (required)
    pub mnemonic: String,

    /// Optional passphrase for the mnemonic
    #[serde(default)]
    pub passphrase: Option<String>,

    /// Working directory for all data (SDK storage, database, etc.)
    #[serde(default = "default_working_dir")]
    pub working_dir: String,
}

impl BackendConfig {
    /// Get the storage directory for Breez SDK data
    pub fn storage_dir(&self) -> String {
        format!("{}/breez", self.working_dir)
    }

    /// Get the path to the quotes database
    pub fn db_path(&self) -> String {
        format!("{}/quotes.db", self.working_dir)
    }
}

fn default_working_dir() -> String {
    if let Some(home_dir) = home::home_dir() {
        home_dir
            .join(".cdk-spark-payment-processor")
            .to_string_lossy()
            .to_string()
    } else {
        "./.data".to_string()
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            mnemonic: String::new(),
            passphrase: None,
            working_dir: default_working_dir(),
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

        // Check WORKING_DIR env var first to determine config file location
        let working_dir = std::env::var("WORKING_DIR").unwrap_or_else(|_| default_working_dir());

        let config_path = format!("{}/config.toml", working_dir);

        if std::path::Path::new(&config_path).exists() {
            tracing::info!("Loading configuration from {}", config_path);
            fig = fig.merge(Toml::file(&config_path));
        } else {
            tracing::warn!(
                "Configuration file {} not found, using defaults and environment variables",
                config_path
            );
        }

        let mut cfg: Config = fig.extract().unwrap_or_default();

        // 2) Overlay environment variables explicitly
        // Breez-specific environment variables
        if let Ok(v) = std::env::var("BREEZ_API_KEY") {
            tracing::debug!("BREEZ_API_KEY loaded from environment");
            cfg.backend.api_key = v;
        }
        if let Ok(v) = std::env::var("BREEZ_MNEMONIC") {
            tracing::debug!("BREEZ_MNEMONIC loaded from environment");
            cfg.backend.mnemonic = v;
        }
        if let Ok(v) = std::env::var("BREEZ_PASSPHRASE") {
            tracing::debug!("BREEZ_PASSPHRASE loaded from environment");
            cfg.backend.passphrase = Some(v);
        }
        // Ensure working_dir is set from env var (in case config file had different value)
        if let Ok(v) = std::env::var("WORKING_DIR") {
            tracing::debug!("WORKING_DIR loaded from environment: {}", v);
            cfg.backend.working_dir = v;
        }
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
