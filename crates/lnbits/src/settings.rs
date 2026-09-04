use anyhow::{Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Deserializer, Serialize};

const BACKEND_ENV_PREFIX: &str = "LNBITS_";
const BACKEND_CONFIG_SECTION: &str = "lnbits";

/// LNbits wallet connection and fee configuration.
#[derive(Clone, Serialize)]
pub struct BackendConfig {
    /// LNbits wallet admin key. This key is required to send payments.
    pub admin_api_key: String,
    /// LNbits wallet invoice/read key.
    pub invoice_api_key: String,
    /// LNbits instance URL, either at its root or ending in `/api/v1`.
    pub api_url: String,
    /// Minimum absolute fee reserve for melt quotes, in satoshis.
    pub fee_reserve_min_sat: u64,
    /// Relative fee reserve for melt quotes (`0.02` means two percent).
    pub fee_reserve_percent: f32,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct BackendConfigRepr {
    admin_api_key: String,
    invoice_api_key: String,
    api_url: Option<String>,
    lnbits_api: Option<String>,
    fee_reserve_min_sat: Option<u64>,
    reserve_fee_min: Option<u64>,
    fee_reserve_percent: Option<f32>,
    fee_percent: Option<f32>,
}

impl<'de> Deserialize<'de> for BackendConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config = BackendConfigRepr::deserialize(deserializer)?;

        Ok(Self {
            admin_api_key: config.admin_api_key,
            invoice_api_key: config.invoice_api_key,
            // Canonical names take precedence when a configuration contains
            // both the current and former CDK field names.
            api_url: config.api_url.or(config.lnbits_api).unwrap_or_default(),
            fee_reserve_min_sat: config
                .fee_reserve_min_sat
                .or(config.reserve_fee_min)
                .unwrap_or_else(default_fee_reserve_min_sat),
            fee_reserve_percent: config
                .fee_reserve_percent
                .or(config.fee_percent)
                .unwrap_or_else(default_fee_reserve_percent),
        })
    }
}

impl std::fmt::Debug for BackendConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackendConfig")
            .field("admin_api_key", &"[REDACTED]")
            .field("invoice_api_key", &"[REDACTED]")
            .field("api_url", &self.api_url)
            .field("fee_reserve_min_sat", &self.fee_reserve_min_sat)
            .field("fee_reserve_percent", &self.fee_reserve_percent)
            .finish()
    }
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            admin_api_key: String::new(),
            invoice_api_key: String::new(),
            api_url: String::new(),
            fee_reserve_min_sat: default_fee_reserve_min_sat(),
            fee_reserve_percent: default_fee_reserve_percent(),
        }
    }
}

impl BackendConfig {
    fn is_default(&self) -> bool {
        self.admin_api_key.is_empty()
            && self.invoice_api_key.is_empty()
            && self.api_url.is_empty()
            && self.fee_reserve_min_sat == default_fee_reserve_min_sat()
            && self.fee_reserve_percent == default_fee_reserve_percent()
    }

    /// Validate required fields and fee settings.
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.admin_api_key.trim().is_empty(),
            "lnbits.admin_api_key is required"
        );
        anyhow::ensure!(
            !self.invoice_api_key.trim().is_empty(),
            "lnbits.invoice_api_key is required"
        );
        anyhow::ensure!(
            !self.api_url.trim().is_empty(),
            "lnbits.api_url is required"
        );
        anyhow::ensure!(
            self.fee_reserve_percent.is_finite() && self.fee_reserve_percent >= 0.0,
            "lnbits.fee_reserve_percent must be a finite, non-negative number"
        );
        Ok(())
    }
}

fn default_fee_reserve_min_sat() -> u64 {
    2
}

fn default_fee_reserve_percent() -> f32 {
    0.02
}

/// Main configuration: `config.toml` overlaid by environment variables.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// LNbits backend configuration.
    #[serde(default, skip_serializing_if = "BackendConfig::is_default")]
    pub lnbits: BackendConfig,
    /// gRPC listen address.
    #[serde(default = "default_address")]
    pub address: String,
    /// gRPC listen port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Enable mutual TLS for the payment processor gRPC server.
    #[serde(default)]
    pub tls_enable: bool,
    /// Explicitly allow plaintext gRPC.
    #[serde(default)]
    pub allow_insecure: bool,
    /// Payment processor server certificate path.
    #[serde(default = "default_tls_cert_path")]
    pub tls_cert_path: String,
    /// Payment processor server private-key path.
    #[serde(default = "default_tls_key_path")]
    pub tls_key_path: String,
    /// CA certificate used to authenticate mint clients.
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
            lnbits: BackendConfig::default(),
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
    /// Load `config.toml`, if present, then apply environment overrides.
    pub fn load() -> Result<Self> {
        let config = extract_config(config_figment())?;
        config.lnbits.validate()?;
        Ok(config)
    }

    /// Load configuration from the supported file and environment sources.
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
            Env::prefixed(BACKEND_ENV_PREFIX)
                .map(|key| format!("{BACKEND_CONFIG_SECTION}.{}", key.as_str()).into()),
        )
}

fn extract_config(figment: Figment) -> Result<Config> {
    figment.extract().context("failed to parse configuration")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_backend_config() -> BackendConfig {
        BackendConfig {
            admin_api_key: "admin-key".to_string(),
            invoice_api_key: "invoice-key".to_string(),
            api_url: "https://lnbits.example.com".to_string(),
            ..BackendConfig::default()
        }
    }

    #[test]
    fn defaults_match_the_documented_fee_reserve() {
        let config = BackendConfig::default();
        assert_eq!(config.fee_reserve_min_sat, 2);
        assert_eq!(config.fee_reserve_percent, 0.02);
    }

    #[test]
    fn debug_output_redacts_both_api_keys() {
        let config = valid_backend_config();
        let debug = format!("{config:?}");

        assert!(!debug.contains("admin-key"));
        assert!(!debug.contains("invoice-key"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn validation_rejects_missing_credentials() {
        let error = BackendConfig::default().validate().unwrap_err();
        assert!(error.to_string().contains("admin_api_key"));
    }

    #[test]
    fn validation_rejects_invalid_fee_percentages() {
        let mut config = valid_backend_config();
        config.fee_reserve_percent = f32::NAN;
        assert!(config.validate().is_err());

        config.fee_reserve_percent = -0.01;
        assert!(config.validate().is_err());
    }

    #[test]
    fn legacy_backend_field_names_remain_accepted() {
        let figment = Figment::from(Serialized::defaults(Config::default())).merge(Toml::string(
            r#"
                [lnbits]
                admin_api_key = "admin"
                invoice_api_key = "invoice"
                lnbits_api = "https://lnbits.example.com/api/v1"
                reserve_fee_min = 7
                fee_percent = 0.03
            "#,
        ));

        let config = extract_config(figment).unwrap();
        assert_eq!(config.lnbits.api_url, "https://lnbits.example.com/api/v1");
        assert_eq!(config.lnbits.fee_reserve_min_sat, 7);
        assert_eq!(config.lnbits.fee_reserve_percent, 0.03);
    }

    #[test]
    fn canonical_backend_fields_use_fee_defaults() {
        let figment = Figment::from(Serialized::defaults(Config::default())).merge(Toml::string(
            r#"
                [lnbits]
                admin_api_key = "admin"
                invoice_api_key = "invoice"
                api_url = "https://lnbits.example.com"
            "#,
        ));

        let config = extract_config(figment).unwrap();
        assert_eq!(config.lnbits.fee_reserve_min_sat, 2);
        assert_eq!(config.lnbits.fee_reserve_percent, 0.02);
    }
}
