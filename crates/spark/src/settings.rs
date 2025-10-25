use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Backend-specific configuration
///
/// Configuration for Breez SDK Spark
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// Breez API key (required)
    pub api_key: String,

    /// Mnemonic seed phrase for the wallet (required)
    pub mnemonic: String,

    /// Optional passphrase for the mnemonic
    #[serde(default)]
    pub passphrase: Option<String>,

    /// Storage directory for SDK data
    #[serde(default = "default_storage_dir")]
    pub storage_dir: String,

    /// Database path for quote mappings
    #[serde(default = "default_db_path")]
    pub db_path: String,
}

fn default_storage_dir() -> String {
    "./.data".to_string()
}

fn default_db_path() -> String {
    "./.data/quotes.db".to_string()
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            mnemonic: String::new(),
            passphrase: None,
            storage_dir: default_storage_dir(),
            db_path: default_db_path(),
        }
    }
}

/// Main configuration structure
///
/// Loads configuration from config.toml and environment variables.
/// Environment variables take precedence over file configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Backend type identifier (e.g., "blink", "lnd", "cln", "mock")
    #[serde(default)]
    pub backend_type: String,

    /// Backend-specific configuration
    #[serde(default)]
    pub backend: BackendConfig,

    /// gRPC server port
    pub server_port: u16,

    /// TLS config for gRPC server
    pub tls_enable: bool,
    pub tls_cert_path: String,
    pub tls_key_path: String,

    /// HTTP/2 keep-alive interval (e.g., "30s")
    #[serde(default)]
    pub keep_alive_interval: Option<String>,

    /// HTTP/2 keep-alive timeout (e.g., "10s")
    #[serde(default)]
    pub keep_alive_timeout: Option<String>,

    /// Maximum connection age (e.g., "30m")
    #[serde(default)]
    pub max_connection_age: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend_type: "mock".to_string(),
            backend: BackendConfig::default(),
            server_port: 50051,
            tls_enable: false,
            tls_cert_path: "certs/server.crt".to_string(),
            tls_key_path: "certs/server.key".to_string(),
            keep_alive_interval: None,
            keep_alive_timeout: None,
            max_connection_age: None,
        }
    }
}

impl Config {
    /// Load from config.toml (if present) and environment variables.
    /// Environment variables override file values.
    ///
    /// # TODO
    /// Add environment variable loading for your backend-specific configuration
    ///
    /// # Example
    /// ```rust,ignore
    /// if let Ok(v) = std::env::var("API_URL") {
    ///     cfg.api_url = v;
    /// }
    /// if let Ok(v) = std::env::var("API_KEY") {
    ///     cfg.api_key = v;
    /// }
    /// ```
    pub fn load() -> Self {
        // 1) Start with defaults + config.toml only if it exists
        let base: Config = Default::default();
        let mut fig = Figment::from(Serialized::defaults(base));

        let config_path = "config.toml";
        if std::path::Path::new(config_path).exists() {
            tracing::info!("Loading configuration from {}", config_path);
            fig = fig.merge(Toml::file(config_path));
        } else {
            tracing::warn!("Configuration file {} not found, using defaults and environment variables", config_path);
        }

        let mut cfg: Config = fig.extract().unwrap_or_default();

        tracing::debug!("Initial config loaded - server_port: {}, tls_enable: {}",
            cfg.server_port, cfg.tls_enable);

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
        if let Ok(v) = std::env::var("BREEZ_STORAGE_DIR") {
            tracing::debug!("BREEZ_STORAGE_DIR loaded from environment: {}", v);
            cfg.backend.storage_dir = v;
        }
        if let Ok(v) = std::env::var("BREEZ_DB_PATH") {
            tracing::debug!("BREEZ_DB_PATH loaded from environment: {}", v);
            cfg.backend.db_path = v;
        }

        // Server configuration
        if let Ok(v) = std::env::var("SERVER_PORT") {
            cfg.server_port = v.parse().unwrap_or(cfg.server_port);
            tracing::debug!("SERVER_PORT loaded from environment: {}", cfg.server_port);
        }
        if let Ok(v) = std::env::var("TLS_ENABLE") {
            cfg.tls_enable = matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES");
            tracing::debug!("TLS_ENABLE loaded from environment: {}", cfg.tls_enable);
        }
        if let Ok(v) = std::env::var("TLS_CERT_PATH") {
            cfg.tls_cert_path = v;
        }
        if let Ok(v) = std::env::var("TLS_KEY_PATH") {
            cfg.tls_key_path = v;
        }

        // Log final configuration summary (without sensitive data)
        tracing::info!("Configuration loaded - storage_dir: {}, server_port: {}",
            cfg.backend.storage_dir, cfg.server_port);
        tracing::debug!("API key present: {}, Mnemonic present: {}",
            !cfg.backend.api_key.is_empty(), !cfg.backend.mnemonic.is_empty());

        cfg
    }

    pub fn from_env() -> Self {
        Self::load()
    }
}
