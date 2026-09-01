use anyhow::{bail, Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use spark_wallet::Network;

const BACKEND_ENV_PREFIX: &str = "SPARK_";

/// A single Spark operator in a custom federation
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OperatorSettings {
    /// gRPC address of the Spark operator
    pub address: String,

    /// FROST identifier of the operator (32-byte hex)
    pub identifier: String,

    /// Identity public key of the operator (33-byte compressed hex)
    pub identity_public_key: String,

    /// Path to a PEM CA certificate used to verify the operator's TLS
    /// connection; required when the operator does not use a publicly
    /// trusted certificate
    #[serde(default)]
    pub ca_cert_path: Option<String>,
}

/// Custom Spark Service Provider (SSP) used for Lightning swaps
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SspSettings {
    /// Base URL of the SSP API
    pub base_url: String,

    /// Identity public key of the SSP (33-byte compressed hex)
    pub identity_public_key: String,

    /// Optional GraphQL schema endpoint path
    #[serde(default)]
    pub schema_endpoint: Option<String>,
}

/// Backend-specific configuration for Spark wallet
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    /// BIP39 mnemonic for wallet seed
    pub mnemonic: String,

    /// Spark network to connect to (mainnet, regtest, testnet, signet)
    #[serde(default = "default_network")]
    pub network: Network,

    /// Data directory for persistent quote and Spark request mappings
    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    /// Custom operator federation. When set, replaces the default operator
    /// pool; required for networks without public operators such as regtest.
    /// Operators are indexed by their position in the list (id 0, 1, ...).
    #[serde(default)]
    pub operators: Vec<OperatorSettings>,

    /// Number of operators whose key shares are needed to reconstruct the
    /// wallet secret. Defaults to 2 (or the operator count if fewer).
    #[serde(default)]
    pub split_secret_threshold: Option<u32>,

    /// Custom Spark Service Provider (SSP) used for Lightning swaps. When
    /// unset, the default SSP for the selected network is used.
    #[serde(default)]
    pub ssp: Option<SspSettings>,
}

fn default_network() -> Network {
    Network::Mainnet
}

fn default_data_dir() -> String {
    ".data/spark".to_string()
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            mnemonic: String::new(),
            network: default_network(),
            data_dir: default_data_dir(),
            operators: Vec::new(),
            split_secret_threshold: None,
            ssp: None,
        }
    }
}

impl BackendConfig {
    /// Resolve the signing threshold against the effective operator count.
    pub fn resolve_split_secret_threshold(&self, operator_count: usize) -> Result<u32> {
        let count = u32::try_from(operator_count).context("too many operators")?;
        let threshold = self.split_secret_threshold.unwrap_or(count.min(2));
        if threshold == 0 {
            bail!("split_secret_threshold must be at least 1");
        }
        if threshold > count {
            bail!(
                "split_secret_threshold ({threshold}) cannot exceed the number of operators ({count})"
            );
        }
        Ok(threshold)
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
    /// PEM CA certificate used to authenticate mint clients.
    #[serde(default = "default_tls_client_ca_path")]
    pub tls_client_ca_path: String,
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

fn default_tls_client_ca_path() -> String {
    "certs/ca.pem".to_string()
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
            tls_client_ca_path: default_tls_client_ca_path(),
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
