use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Backend-specific configuration for Ark (Bark) wallet
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// BIP39 mnemonic for wallet seed
    pub mnemonic: String,

    /// Ark server address
    #[serde(default = "default_server_address")]
    pub server_address: String,

    /// Esplora API address
    #[serde(default = "default_esplora_address")]
    pub esplora_address: String,

    /// Bitcoin network (signet, testnet, mainnet)
    #[serde(default = "default_network")]
    pub network: String,

    /// Data directory for SQLite database
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

fn default_server_address() -> String {
    "https://ark.signet.2nd.dev".to_string()
}

fn default_esplora_address() -> String {
    "https://esplora.signet.2nd.dev".to_string()
}

fn default_network() -> String {
    "signet".to_string()
}

fn default_data_dir() -> String {
    ".data/bark".to_string()
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            mnemonic: String::new(),
            server_address: default_server_address(),
            esplora_address: default_esplora_address(),
            network: default_network(),
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
    /// Backend type identifier (e.g., "ark")
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
            backend_type: "ark".to_string(),
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
    pub fn load() -> Self {
        // 1) Start with defaults + config.toml only if it exists
        let base: Config = Default::default();
        let mut fig = Figment::from(Serialized::defaults(base));
        if std::path::Path::new("config.toml").exists() {
            fig = fig.merge(Toml::file("config.toml"));
        }
        let mut cfg: Config = fig.extract().unwrap_or_default();

        // 2) Overlay environment variables explicitly
        if let Ok(v) = std::env::var("MNEMONIC") {
            cfg.backend.mnemonic = v;
        }
        if let Ok(v) = std::env::var("ARK_SERVER_ADDRESS") {
            cfg.backend.server_address = v;
        }
        if let Ok(v) = std::env::var("ESPLORA_ADDRESS") {
            cfg.backend.esplora_address = v;
        }
        if let Ok(v) = std::env::var("NETWORK") {
            cfg.backend.network = v;
        }
        if let Ok(v) = std::env::var("DATA_DIR") {
            cfg.backend.data_dir = v;
        }
        if let Ok(v) = std::env::var("SERVER_PORT") {
            cfg.server_port = v.parse().unwrap_or(cfg.server_port);
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
