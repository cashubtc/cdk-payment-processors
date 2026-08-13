use anyhow::{bail, Context, Result};
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Backend-specific configuration for Bark wallet
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// BIP39 mnemonic for wallet seed
    pub mnemonic: String,

    /// Bark server address
    #[serde(default = "default_server_address")]
    pub server_address: String,

    /// Esplora API address
    #[serde(default = "default_esplora_address")]
    pub esplora_address: String,

    /// Bitcoin network (mainnet, testnet, signet, regtest)
    #[serde(default = "default_network")]
    pub network: String,

    /// Data directory for SQLite database
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

fn default_server_address() -> String {
    "https://ark.second.tech".to_string()
}

fn default_esplora_address() -> String {
    "https://mempool.second.tech/api".to_string()
}

fn default_network() -> String {
    "mainnet".to_string()
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
    #[serde(default)]
    pub tls_enable: bool,
    #[serde(default = "default_tls_cert_path")]
    pub tls_cert_path: String,
    #[serde(default = "default_tls_key_path")]
    pub tls_key_path: String,
}

fn default_address() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    50051
}

fn default_tls_cert_path() -> String {
    "certs/server.crt".to_string()
}

fn default_tls_key_path() -> String {
    "certs/server.key".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend: BackendConfig::default(),
            address: default_address(),
            port: default_port(),
            tls_enable: false,
            tls_cert_path: default_tls_cert_path(),
            tls_key_path: default_tls_key_path(),
        }
    }
}

impl Config {
    /// Load from config.toml (if present) and environment variables.
    /// Environment variables override file values.
    pub fn load() -> Result<Self> {
        // 1) Start with defaults + config.toml only if it exists
        let base: Config = Default::default();
        let mut fig = Figment::from(Serialized::defaults(base));
        if std::path::Path::new("config.toml").exists() {
            fig = fig.merge(Toml::file("config.toml"));
        }
        let mut cfg = extract_config(fig)?;

        // 2) Overlay environment variables explicitly
        if let Ok(v) = std::env::var("BARK_MNEMONIC") {
            cfg.backend.mnemonic = v;
        }
        if let Ok(v) = std::env::var("BARK_SERVER_ADDRESS") {
            cfg.backend.server_address = v;
        }
        if let Ok(v) = std::env::var("BARK_ESPLORA_ADDRESS") {
            cfg.backend.esplora_address = v;
        }
        if let Ok(v) = std::env::var("BARK_NETWORK") {
            cfg.backend.network = v;
        }
        if let Ok(v) = std::env::var("BARK_DATA_DIR") {
            cfg.backend.data_dir = v;
        }

        // Server configuration
        if let Ok(v) = std::env::var("SERVER_ADDRESS") {
            cfg.address = v;
        }
        if let Ok(v) = std::env::var("SERVER_PORT") {
            cfg.port = v
                .parse()
                .with_context(|| format!("invalid SERVER_PORT value `{v}`"))?;
        }
        if let Ok(v) = std::env::var("TLS_ENABLE") {
            cfg.tls_enable = parse_bool_env("TLS_ENABLE", &v)?;
        }
        if let Ok(v) = std::env::var("TLS_CERT_PATH") {
            cfg.tls_cert_path = v;
        }
        if let Ok(v) = std::env::var("TLS_KEY_PATH") {
            cfg.tls_key_path = v;
        }

        Ok(cfg)
    }

    pub fn from_env() -> Result<Self> {
        Self::load()
    }
}

fn extract_config(figment: Figment) -> Result<Config> {
    figment.extract().context("failed to parse configuration")
}

fn parse_bool_env(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("invalid {name} value `{value}`; expected true or false"),
    }
}
