use anyhow::{bail, Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

const BACKEND_ENV_PREFIX: &str = "BARK_";

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

    /// Interval between payment event polling passes, in milliseconds
    #[serde(default = "default_event_poll_interval_ms")]
    pub event_poll_interval_ms: u64,
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

fn default_event_poll_interval_ms() -> u64 {
    5_000
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            mnemonic: String::new(),
            server_address: default_server_address(),
            esplora_address: default_esplora_address(),
            network: default_network(),
            data_dir: default_data_dir(),
            event_poll_interval_ms: default_event_poll_interval_ms(),
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
    /// Explicitly allow plaintext gRPC.
    #[serde(default)]
    pub allow_insecure: bool,
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
            allow_insecure: false,
            tls_cert_path: default_tls_cert_path(),
            tls_key_path: default_tls_key_path(),
        }
    }
}

impl Config {
    /// Load from config.toml (if present) and environment variables.
    /// Environment variables override file values.
    pub fn load() -> Result<Self> {
        let cfg = extract_config(config_figment())?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_env() -> Result<Self> {
        Self::load()
    }

    fn validate(&self) -> Result<()> {
        if self.backend.event_poll_interval_ms == 0 {
            bail!("BARK_EVENT_POLL_INTERVAL_MS must be greater than zero");
        }
        Ok(())
    }
}

fn config_figment() -> Figment {
    let mut figment = Figment::from(Serialized::defaults(Config::default()));
    if std::path::Path::new("config.toml").is_file() {
        figment = figment.merge(Toml::file_exact("config.toml"));
    }

    figment
        .merge(Env::prefixed("SERVER_"))
        .merge(Env::prefixed("TLS_").map(|key| format!("tls_{}", key.as_str()).into()))
        .merge(Env::raw().only(&["ALLOW_INSECURE"]))
        .merge(
            Env::prefixed(BACKEND_ENV_PREFIX).map(|key| format!("backend.{}", key.as_str()).into()),
        )
}

fn extract_config(figment: Figment) -> Result<Config> {
    figment.extract().context("failed to parse configuration")
}
