use std::fmt;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{de, Deserialize, Deserializer, Serialize};

const BACKEND_ENV_PREFIX: &str = "BARK_";
const BACKEND_CONFIG_SECTION: &str = "bark";

/// A payment method this backend can advertise to a mint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AdvertisedMethod {
    /// Lightning, settled through the Ark server.
    Bolt11,
    /// On-chain, boarding the deposit into Ark.
    Onchain,
    /// Arkoor, exposed as a CDK custom method.
    Arkoor,
}

impl AdvertisedMethod {
    /// Every method this backend is able to serve.
    pub const ALL: [AdvertisedMethod; 3] = [Self::Bolt11, Self::Onchain, Self::Arkoor];

    /// The method name as CDK spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bolt11 => "bolt11",
            Self::Onchain => "onchain",
            Self::Arkoor => "arkoor",
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
            "onchain" => Ok(Self::Onchain),
            "arkoor" => Ok(Self::Arkoor),
            other => bail!(
                "unknown payment method {other:?}; supported methods are bolt11, onchain, arkoor"
            ),
        }
    }
}

/// Accept either a TOML list (`["onchain"]`) or a comma-separated string
/// (`BARK_PAYMENT_METHODS=onchain,bolt11`), so the setting reads naturally from
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

    /// Payment methods this backend advertises to the mint.
    ///
    /// A CDK mint picks its backend per `(unit, method)` pair and claims every
    /// method a backend advertises in its settings. Leaving this empty
    /// advertises everything bark supports, which is what a mint backed only by
    /// bark wants. Naming a subset lets bark run *alongside* another backend —
    /// for example a Core Lightning node keeping `bolt11` while bark serves
    /// `onchain` — which is otherwise impossible, because both backends would
    /// claim `bolt11` and the mint rejects the duplicate pair.
    #[serde(default, deserialize_with = "deserialize_payment_methods")]
    pub payment_methods: Vec<String>,
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
                .with_context(|| format!("invalid entry in bark.payment_methods: {name:?}"))?;
            if !methods.contains(&method) {
                methods.push(method);
            }
        }
        Ok(methods)
    }
}

/// Main configuration structure
///
/// Loads configuration from config.toml and environment variables.
/// Environment variables take precedence over file configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Bark-specific configuration
    #[serde(default)]
    pub bark: BackendConfig,

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
            bark: BackendConfig::default(),
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
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_env() -> Result<Self> {
        Self::load()
    }

    fn validate(&self) -> Result<()> {
        if self.bark.event_poll_interval_ms == 0 {
            bail!("BARK_EVENT_POLL_INTERVAL_MS must be greater than zero");
        }
        // Fail at startup rather than silently advertising the wrong set: a
        // misspelled method would otherwise just go missing from the mint.
        self.bark.advertised_methods()?;
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

    fn config_with(methods: &[&str]) -> BackendConfig {
        BackendConfig {
            payment_methods: methods.iter().map(|m| m.to_string()).collect(),
            ..BackendConfig::default()
        }
    }

    #[test]
    fn unset_payment_methods_advertise_everything() {
        // The default has to stay backwards compatible: a mint backed only by
        // bark expects every rail without configuring anything.
        assert_eq!(
            BackendConfig::default().advertised_methods().unwrap(),
            AdvertisedMethod::ALL.to_vec()
        );
    }

    #[test]
    fn a_subset_advertises_only_that_subset() {
        assert_eq!(
            config_with(&["onchain"]).advertised_methods().unwrap(),
            vec![AdvertisedMethod::Onchain]
        );
    }

    #[test]
    fn method_names_are_trimmed_and_case_insensitive() {
        assert_eq!(
            config_with(&[" OnChain ", "BOLT11"])
                .advertised_methods()
                .unwrap(),
            vec![AdvertisedMethod::Onchain, AdvertisedMethod::Bolt11]
        );
    }

    #[test]
    fn duplicate_entries_are_collapsed() {
        assert_eq!(
            config_with(&["onchain", "onchain"])
                .advertised_methods()
                .unwrap(),
            vec![AdvertisedMethod::Onchain]
        );
    }

    #[test]
    fn an_unknown_method_is_rejected_rather_than_ignored() {
        // Silently dropping a typo would leave the rail missing from the mint
        // with nothing to explain why.
        let err = config_with(&["onchian"])
            .advertised_methods()
            .expect_err("a misspelled method must not be silently ignored");
        let message = format!("{err:#}");
        assert!(message.contains("onchian"), "{message}");
        assert!(message.contains("bolt11, onchain, arkoor"), "{message}");
    }

    #[test]
    fn validation_rejects_an_unknown_method_at_startup() {
        let config = Config {
            bark: config_with(&["lightning"]),
            ..Config::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_comma_separated_string_parses_like_a_list() {
        // `BARK_PAYMENT_METHODS=onchain,bolt11` has to work as well as a TOML list.
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(deserialize_with = "deserialize_payment_methods")]
            payment_methods: Vec<String>,
        }

        let from_string: Wrapper =
            serde_json::from_str(r#"{"payment_methods": "onchain, bolt11"}"#).unwrap();
        let from_list: Wrapper =
            serde_json::from_str(r#"{"payment_methods": ["onchain", "bolt11"]}"#).unwrap();
        assert_eq!(from_string.payment_methods, vec!["onchain", "bolt11"]);
        assert_eq!(from_string.payment_methods, from_list.payment_methods);
    }

    #[test]
    fn the_environment_variable_reaches_the_bark_config() {
        // Proves the whole path: BARK_PAYMENT_METHODS -> figment -> BackendConfig.
        // figment's `parse-value` feature may hand the deserializer either a
        // string or an already-split sequence; both are accepted, and this test
        // pins whichever one it actually does.
        //
        // Sole env-touching test in this module, so it cannot race the others.
        std::env::set_var("BARK_PAYMENT_METHODS", "onchain, bolt11");
        let extracted = extract_config(config_figment());
        std::env::remove_var("BARK_PAYMENT_METHODS");

        let config = extracted.expect("config with BARK_PAYMENT_METHODS set should parse");
        assert_eq!(
            config.bark.advertised_methods().unwrap(),
            vec![AdvertisedMethod::Onchain, AdvertisedMethod::Bolt11]
        );
    }

    #[test]
    fn an_empty_string_is_treated_as_unset() {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(deserialize_with = "deserialize_payment_methods")]
            payment_methods: Vec<String>,
        }

        let parsed: Wrapper = serde_json::from_str(r#"{"payment_methods": ""}"#).unwrap();
        assert!(parsed.payment_methods.is_empty());
    }
}
