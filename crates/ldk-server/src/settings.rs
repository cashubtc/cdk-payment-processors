use std::fmt;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{de, Deserialize, Deserializer, Serialize};

const BACKEND_ENV_PREFIX: &str = "LDK_";

/// A payment method this backend can advertise to a mint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AdvertisedMethod {
    /// Lightning BOLT11 invoices.
    Bolt11,
    /// Lightning BOLT12 offers.
    Bolt12,
}

impl AdvertisedMethod {
    /// Every method this backend is able to serve.
    pub const ALL: [AdvertisedMethod; 2] = [Self::Bolt11, Self::Bolt12];

    /// The method name as CDK spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bolt11 => "bolt11",
            Self::Bolt12 => "bolt12",
        }
    }
}

impl fmt::Display for AdvertisedMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AdvertisedMethod {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "bolt11" => Ok(Self::Bolt11),
            "bolt12" => Ok(Self::Bolt12),
            other => {
                bail!("unknown payment method {other:?}; supported methods are bolt11, bolt12")
            }
        }
    }
}

/// Accept either a TOML list (`["bolt11"]`) or a comma-separated string
/// (`LDK_PAYMENT_METHODS=bolt11,bolt12`), so the setting reads naturally from
/// both a config file and the environment.
fn deserialize_payment_methods<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MethodList;

    impl<'de> de::Visitor<'de> for MethodList {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a list of payment method names, or a comma-separated string")
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            Ok(split_method_list(value))
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut methods = Vec::new();
            while let Some(entry) = seq.next_element::<String>()? {
                methods.extend(split_method_list(&entry));
            }
            Ok(methods)
        }
    }

    deserializer.deserialize_any(MethodList)
}

fn split_method_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
        .collect()
}

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

    /// Payment methods this backend advertises to the mint.
    ///
    /// A CDK mint picks its backend per `(unit, method)` pair and claims every
    /// method a backend advertises in its settings. Leaving this empty
    /// advertises everything this backend supports. Naming a subset lets it run
    /// alongside another backend that would otherwise claim the same rail — an
    /// on-chain processor offering `bolt11`, for instance — because the mint
    /// rejects a duplicate `(unit, method)` pair.
    #[serde(default, deserialize_with = "deserialize_payment_methods")]
    pub payment_methods: Vec<String>,
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
            payment_methods: Vec::new(),
        }
    }
}

impl BackendConfig {
    /// The methods this backend should advertise, validated.
    ///
    /// An empty configuration means "everything supported" — advertising
    /// nothing would leave the mint with no rail to register, so it is treated
    /// as unset rather than as an empty allow-list.
    pub fn advertised_methods(&self) -> Result<Vec<AdvertisedMethod>> {
        if self.payment_methods.is_empty() {
            return Ok(AdvertisedMethod::ALL.to_vec());
        }

        let mut methods = Vec::with_capacity(self.payment_methods.len());
        for name in &self.payment_methods {
            let method = name
                .parse::<AdvertisedMethod>()
                .with_context(|| format!("invalid entry in backend.payment_methods: {name:?}"))?;
            if !methods.contains(&method) {
                methods.push(method);
            }
        }
        Ok(methods)
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
        // Fail at startup rather than silently advertising the wrong set.
        cfg.backend.advertised_methods()?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(methods: &[&str]) -> BackendConfig {
        BackendConfig {
            payment_methods: methods.iter().map(|m| m.to_string()).collect(),
            ..BackendConfig::default()
        }
    }

    #[test]
    fn unset_payment_methods_advertise_everything() {
        assert_eq!(
            BackendConfig::default().advertised_methods().unwrap(),
            AdvertisedMethod::ALL.to_vec()
        );
    }

    #[test]
    fn a_subset_advertises_only_that_subset() {
        // Leaves bolt11 free for a backend that can only serve that rail.
        assert_eq!(
            config_with(&["bolt12"]).advertised_methods().unwrap(),
            vec![AdvertisedMethod::Bolt12]
        );
    }

    #[test]
    fn method_names_are_trimmed_and_case_insensitive() {
        assert_eq!(
            config_with(&[" BOLT11 ", "Bolt12"])
                .advertised_methods()
                .unwrap(),
            vec![AdvertisedMethod::Bolt11, AdvertisedMethod::Bolt12]
        );
    }

    #[test]
    fn duplicate_entries_are_collapsed() {
        assert_eq!(
            config_with(&["bolt11", "bolt11"])
                .advertised_methods()
                .unwrap(),
            vec![AdvertisedMethod::Bolt11]
        );
    }

    #[test]
    fn an_unknown_method_is_rejected_rather_than_ignored() {
        let err = config_with(&["onchain"])
            .advertised_methods()
            .expect_err("this backend cannot serve on-chain and must say so");
        let message = format!("{err:#}");
        assert!(message.contains("onchain"), "{message}");
        assert!(message.contains("bolt11, bolt12"), "{message}");
    }

    #[test]
    fn a_comma_separated_value_splits_and_trims() {
        assert_eq!(
            split_method_list(" bolt11 , bolt12 "),
            vec!["bolt11".to_string(), "bolt12".to_string()]
        );
        assert!(split_method_list("").is_empty());
        assert!(split_method_list(" , ").is_empty());
    }

    #[test]
    fn the_environment_variable_reaches_the_backend_config() {
        // Proves the whole path: LDK_PAYMENT_METHODS -> figment -> BackendConfig.
        // figment's `parse-value` feature may hand the deserializer a string or
        // an already-split sequence; both are accepted.
        //
        // Sole env-touching test in this module, so it cannot race the others.
        std::env::set_var("LDK_PAYMENT_METHODS", "bolt12");
        let extracted = extract_config(config_figment());
        std::env::remove_var("LDK_PAYMENT_METHODS");

        let config = extracted.expect("config with LDK_PAYMENT_METHODS set should parse");
        assert_eq!(
            config.backend.advertised_methods().unwrap(),
            vec![AdvertisedMethod::Bolt12]
        );
    }
}
