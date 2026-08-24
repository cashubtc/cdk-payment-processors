use anyhow::{Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

const BACKEND_ENV_PREFIX: &str = "TEMPLATE_";

/// Backend-specific configuration
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct BackendConfig {
    // TODO: Add your backend-specific configuration fields here
    //
    // Example:
    pub api_url: String,
    pub api_key: String,
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
        extract_config(config_figment())
    }

    pub fn from_env() -> Result<Self> {
        Self::load()
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
