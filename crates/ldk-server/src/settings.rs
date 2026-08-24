use anyhow::{Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

const BACKEND_ENV_PREFIX: &str = "LDK_";

/// LDK Server node connection and fee configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// LDK Server gRPC address without scheme, e.g. "127.0.0.1:3536".
    pub address: String,
    /// HMAC API key expected by LDK Server (64-char hex).
    pub api_key: String,
    /// Path to the PEM TLS certificate to pin for the LDK Server connection.
    pub tls_cert_path: String,
    /// Minimum absolute fee reserve for melt quotes, in satoshis.
    #[serde(default = "default_fee_reserve_min_sat")]
    pub fee_reserve_min_sat: u64,
    /// Relative fee reserve for melt quotes (0.01 = 1%).
    #[serde(default = "default_fee_reserve_percent")]
    pub fee_reserve_percent: f32,
    /// Maximum ListPayments pages to scan for incoming status lookups.
    #[serde(default = "default_max_payment_scan_pages")]
    pub max_payment_scan_pages: u16,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            api_key: String::new(),
            tls_cert_path: String::new(),
            fee_reserve_min_sat: default_fee_reserve_min_sat(),
            fee_reserve_percent: default_fee_reserve_percent(),
            max_payment_scan_pages: default_max_payment_scan_pages(),
        }
    }
}

fn default_fee_reserve_min_sat() -> u64 {
    2
}

fn default_fee_reserve_percent() -> f32 {
    0.01
}

fn default_max_payment_scan_pages() -> u16 {
    32
}

/// Main configuration: config.toml overlaid by environment variables.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub backend: BackendConfig,
    /// gRPC listen address for the payment processor.
    #[serde(default = "default_address")]
    pub address: String,
    /// gRPC listen port for the payment processor.
    #[serde(default = "default_port")]
    pub port: u16,
    /// TLS for the payment processor gRPC server.
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
        anyhow::ensure!(
            !cfg.backend.address.is_empty(),
            "backend.address is required"
        );
        anyhow::ensure!(
            !cfg.backend.api_key.is_empty(),
            "backend.api_key is required"
        );
        anyhow::ensure!(
            !cfg.backend.tls_cert_path.is_empty(),
            "backend.tls_cert_path is required"
        );
        Ok(cfg)
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
