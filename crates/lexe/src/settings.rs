use anyhow::{bail, Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Environment variable prefix for backend-specific settings.
const BACKEND_ENV_PREFIX: &str = "LEXE_";
const BACKEND_CONFIG_SECTION: &str = "lexe";

/// Networks accepted by the processor.
const NETWORKS: &[&str] = &["mainnet", "testnet3", "regtest"];

/// Backend-specific configuration for the Lexe managed node.
///
/// Credential sources match [lexe-cli](https://github.com/lexe-app/lexe-public/tree/master/lexe-cli):
/// import a client-credentials blob (inline or file), import a root seed
/// (mnemonic or hex, inline or file), or leave every source unset and let
/// the processor create a wallet the way `lexe init` does.
#[derive(Clone, Deserialize, Serialize)]
pub struct BackendConfig {
    /// Base64 Lexe SDK client credentials (Lexe app → Menu → SDK clients, or
    /// `lexe-cli` export).
    ///
    /// Same blob lexe-mcp passes to the sidecar as
    /// `Authorization: Bearer <credentials>`. Mutually exclusive with the
    /// other credential sources.
    #[serde(default)]
    pub client_credentials: Option<String>,
    /// Path to a file containing the client-credentials blob.
    ///
    /// Same role as lexe-cli `--client-credentials-path` /
    /// `LEXE_CLIENT_CREDENTIALS_PATH`.
    #[serde(default)]
    pub client_credentials_path: Option<String>,
    /// BIP39 seed phrase, or a 64-character hex root seed.
    ///
    /// Same role as lexe-cli `--root-seed` / `LEXE_ROOT_SEED`.
    ///
    /// `LEXE_SEED_PHRASE` is accepted as an alias when `LEXE_ROOT_SEED` is
    /// unset, so existing processor configs keep working. Config files should
    /// use `root_seed`.
    #[serde(default)]
    pub root_seed: Option<String>,
    /// Path to a file containing a BIP39 mnemonic or a 64-character hex root seed.
    ///
    /// Same role as lexe-cli `--root-seed-path` / `LEXE_ROOT_SEED_PATH`.
    #[serde(default)]
    pub root_seed_path: Option<String>,
    /// Create a wallet when no credential is configured.
    ///
    /// Default `true`, matching `lexe init`: generate a seed, persist it
    /// under `data_dir`, sign up, and provision. Set to `false` to fail
    /// closed when credentials are missing.
    #[serde(default = "default_create_wallet")]
    pub create_wallet: bool,
    /// Lexe environment: `mainnet`, `testnet3` or `regtest`.
    #[serde(default = "default_network")]
    pub network: String,
    /// Data directory for the Lexe wallet state and the quote database.
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Estimated outgoing fee in ppm used for melt quotes. Lexe has no
    /// fee-estimation API, so quotes use this estimate; the actual fee is
    /// reported once the payment settles.
    #[serde(default = "default_fee_reserve_ppm")]
    pub fee_reserve_ppm: u32,
    /// Minimum estimated outgoing fee in sats.
    #[serde(default = "default_fee_reserve_min_sat")]
    pub fee_reserve_min_sat: u32,
    /// Default timeout in seconds for outgoing payments when the mint does
    /// not provide one. Lexe preflight can take ~60s, so keep this well
    /// above that.
    #[serde(default = "default_payment_timeout_secs")]
    pub payment_timeout_secs: u64,
}

/// `client_credentials` and `seed_phrase` are credential material, so they
/// are redacted in the (manually implemented) debug output.
impl std::fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConfig")
            .field(
                "client_credentials",
                &self.client_credentials.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "client_credentials_path",
                &self.client_credentials_path.as_ref().map(|_| "<redacted>"),
            )
            .field("root_seed", &self.root_seed.as_ref().map(|_| "<redacted>"))
            .field(
                "root_seed_path",
                &self.root_seed_path.as_ref().map(|_| "<redacted>"),
            )
            .field("create_wallet", &self.create_wallet)
            .field("network", &self.network)
            .field("data_dir", &self.data_dir)
            .field("fee_reserve_ppm", &self.fee_reserve_ppm)
            .field("fee_reserve_min_sat", &self.fee_reserve_min_sat)
            .field("payment_timeout_secs", &self.payment_timeout_secs)
            .finish()
    }
}

fn default_network() -> String {
    "mainnet".to_string()
}

fn default_data_dir() -> String {
    ".data/lexe".to_string()
}

fn default_fee_reserve_ppm() -> u32 {
    10_000
}

fn default_fee_reserve_min_sat() -> u32 {
    2
}

fn default_payment_timeout_secs() -> u64 {
    300
}

fn default_create_wallet() -> bool {
    true
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            client_credentials: None,
            client_credentials_path: None,
            root_seed: None,
            root_seed_path: None,
            create_wallet: default_create_wallet(),
            network: default_network(),
            data_dir: default_data_dir(),
            fee_reserve_ppm: default_fee_reserve_ppm(),
            fee_reserve_min_sat: default_fee_reserve_min_sat(),
            payment_timeout_secs: default_payment_timeout_secs(),
        }
    }
}

/// Main configuration: config.toml overlaid by environment variables.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Lexe-specific configuration
    #[serde(default)]
    pub lexe: BackendConfig,
    /// gRPC server address.
    #[serde(default = "default_address")]
    pub address: String,
    /// gRPC server port.
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
            lexe: BackendConfig::default(),
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
        let mut figment = Figment::from(Serialized::defaults(Config::default()));
        if std::path::Path::new("config.toml").is_file() {
            figment = figment.merge(Toml::file_exact("config.toml"));
        }
        Self::from_figment(figment)
    }

    /// Load from environment variables only, ignoring any local
    /// `config.toml`, so unit tests do not depend on the working directory.
    pub fn load_env_only() -> Result<Self> {
        Self::from_figment(Figment::from(Serialized::defaults(Config::default())))
    }

    /// Alias for [`Self::load`].
    pub fn from_env() -> Result<Self> {
        Self::load()
    }

    fn from_figment(figment: Figment) -> Result<Self> {
        let figment = figment
            .merge(Env::prefixed("SERVER_"))
            .merge(Env::prefixed("TLS_").map(|key| format!("tls_{}", key.as_str()).into()))
            .merge(Env::raw().only(&["ALLOW_INSECURE"]))
            .merge(Env::prefixed(BACKEND_ENV_PREFIX).map(|key| {
                // Skip the legacy name here. It is applied below only when
                // LEXE_ROOT_SEED is absent, so the two names cannot collide.
                if key.as_str() == "seed_phrase" {
                    return key.into();
                }
                format!("{BACKEND_CONFIG_SECTION}.{}", key.as_str()).into()
            }));
        let figment = if std::env::var_os("LEXE_ROOT_SEED").is_none() {
            figment.merge(
                Env::raw()
                    .only(&["LEXE_SEED_PHRASE"])
                    .map(|_| format!("{BACKEND_CONFIG_SECTION}.root_seed").into()),
            )
        } else {
            figment
        };
        let cfg = extract_config(figment)?;
        validate(&cfg)?;
        Ok(cfg)
    }
}

fn extract_config(figment: Figment) -> Result<Config> {
    figment.extract().context("failed to parse configuration")
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// The one configured credential, if any. Empty strings do not count.
pub(crate) enum CredentialSource<'a> {
    ClientCredentials(&'a str),
    ClientCredentialsPath(&'a str),
    RootSeed(&'a str),
    RootSeedPath(&'a str),
}

impl<'a> CredentialSource<'a> {
    pub(crate) fn from_backend(backend: &'a BackendConfig) -> Result<Option<Self>> {
        let sources = [
            nonempty(backend.client_credentials.as_deref()).map(Self::ClientCredentials),
            nonempty(backend.client_credentials_path.as_deref()).map(Self::ClientCredentialsPath),
            nonempty(backend.root_seed.as_deref()).map(Self::RootSeed),
            nonempty(backend.root_seed_path.as_deref()).map(Self::RootSeedPath),
        ];
        let mut set = sources.into_iter().flatten();
        let source = set.next();
        if set.next().is_some() {
            bail!(
                "multiple Lexe credential sources; set only one of LEXE_CLIENT_CREDENTIALS, LEXE_CLIENT_CREDENTIALS_PATH, LEXE_ROOT_SEED / LEXE_SEED_PHRASE, LEXE_ROOT_SEED_PATH"
            );
        }
        Ok(source)
    }
}

pub fn validate(cfg: &Config) -> Result<()> {
    if CredentialSource::from_backend(&cfg.lexe)?.is_none() && !cfg.lexe.create_wallet {
        bail!(
            "missing credentials: set LEXE_CLIENT_CREDENTIALS, LEXE_CLIENT_CREDENTIALS_PATH, LEXE_ROOT_SEED / LEXE_SEED_PHRASE, or LEXE_ROOT_SEED_PATH. Or leave them unset with create_wallet = true to generate a wallet like `lexe init`"
        );
    }

    let network = cfg.lexe.network.to_ascii_lowercase();
    if !NETWORKS.contains(&network.as_str()) {
        bail!(
            "invalid LEXE_NETWORK `{}`; expected one of: {}",
            cfg.lexe.network,
            NETWORKS.join(", ")
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    /// Process-wide lock: settings tests mutate environment variables.
    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Clear every LEXE_*/SERVER_*/TLS_*/ALLOW_INSECURE variable the tests use.
    fn clear_test_env() {
        for key in [
            "LEXE_CLIENT_CREDENTIALS",
            "LEXE_CLIENT_CREDENTIALS_PATH",
            "LEXE_ROOT_SEED",
            "LEXE_ROOT_SEED_PATH",
            "LEXE_SEED_PHRASE",
            "LEXE_CREATE_WALLET",
            "LEXE_NETWORK",
            "LEXE_DATA_DIR",
            "LEXE_FEE_RESERVE_PPM",
            "LEXE_FEE_RESERVE_MIN_SAT",
            "LEXE_PAYMENT_TIMEOUT_SECS",
            "SERVER_ADDRESS",
            "SERVER_PORT",
            "TLS_ENABLE",
            "TLS_CERT_PATH",
            "TLS_KEY_PATH",
            "TLS_CLIENT_CA_PATH",
            "ALLOW_INSECURE",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn load_defaults_without_credentials_fails_validation() {
        let _guard = env_lock();
        clear_test_env();

        // Creating a wallet is the default, same as `lexe init`.
        let cfg = Config::load_env_only().expect("create_wallet default should load");
        assert!(cfg.lexe.create_wallet);
        assert!(cfg.lexe.client_credentials.is_none());
        assert!(cfg.lexe.root_seed.is_none());

        std::env::set_var("LEXE_CREATE_WALLET", "false");
        let err = Config::load_env_only().unwrap_err();
        assert!(
            err.to_string().contains("missing credentials"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn client_credentials_accepted() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");

        let cfg = Config::load_env_only().expect("should load with client credentials");
        assert_eq!(cfg.lexe.client_credentials.as_deref(), Some("dGVzdA=="));
        assert_eq!(cfg.lexe.network, "mainnet");
        assert_eq!(cfg.lexe.data_dir, ".data/lexe");
        assert_eq!(cfg.lexe.fee_reserve_ppm, 10_000);
        assert_eq!(cfg.lexe.fee_reserve_min_sat, 2);
        assert_eq!(cfg.lexe.payment_timeout_secs, 300);
        clear_test_env();
    }

    #[test]
    fn seed_phrase_accepted() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_SEED_PHRASE", "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about");

        let cfg = Config::load_env_only().expect("should load with seed phrase");
        assert!(cfg.lexe.client_credentials.is_none());
        assert!(cfg.lexe.root_seed.is_some());
        clear_test_env();
    }

    #[test]
    fn both_credentials_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("LEXE_SEED_PHRASE", "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about");

        let err = Config::load_env_only().unwrap_err();
        assert!(
            err.to_string().contains("multiple Lexe credential sources"),
            "unexpected error: {err}"
        );
        clear_test_env();
    }

    #[test]
    fn invalid_network_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("LEXE_NETWORK", "signet");

        let err = Config::load_env_only().unwrap_err();
        assert!(
            err.to_string().contains("invalid LEXE_NETWORK"),
            "unexpected error: {err}"
        );
        clear_test_env();
    }

    #[test]
    fn env_overrides_defaults() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("LEXE_NETWORK", "testnet3");
        std::env::set_var("LEXE_DATA_DIR", "/tmp/lexe-data");
        std::env::set_var("LEXE_FEE_RESERVE_PPM", "250");
        std::env::set_var("SERVER_PORT", "60001");
        std::env::set_var("ALLOW_INSECURE", "true");

        let cfg = Config::load_env_only().expect("should load with env overrides");
        assert_eq!(cfg.lexe.network, "testnet3");
        assert_eq!(cfg.lexe.data_dir, "/tmp/lexe-data");
        assert_eq!(cfg.lexe.fee_reserve_ppm, 250);
        assert_eq!(cfg.port, 60001);
        assert!(cfg.allow_insecure);
        clear_test_env();
    }

    #[test]
    fn invalid_port_rejected() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS", "dGVzdA==");
        std::env::set_var("SERVER_PORT", "not-a-port");

        assert!(Config::load_env_only().is_err());
        clear_test_env();
    }

    #[test]
    fn credential_file_paths_are_accepted() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_CLIENT_CREDENTIALS_PATH", "/tmp/lexe-cc.txt");

        let cfg = Config::load_env_only().expect("credentials path should load");
        assert_eq!(
            cfg.lexe.client_credentials_path.as_deref(),
            Some("/tmp/lexe-cc.txt")
        );
        assert!(cfg.lexe.root_seed.is_none());

        clear_test_env();
        std::env::set_var("LEXE_ROOT_SEED_PATH", "/tmp/lexe-seed.txt");
        let cfg = Config::load_env_only().expect("root seed path should load");
        assert_eq!(
            cfg.lexe.root_seed_path.as_deref(),
            Some("/tmp/lexe-seed.txt")
        );
        clear_test_env();
    }

    #[test]
    fn seed_phrase_alias_still_loads() {
        let _guard = env_lock();
        clear_test_env();
        std::env::set_var("LEXE_ROOT_SEED", "seed-material");

        let cfg = Config::load_env_only().expect("root seed should load");
        assert_eq!(cfg.lexe.root_seed.as_deref(), Some("seed-material"));
        clear_test_env();
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let config = BackendConfig {
            client_credentials: Some("top-secret-blob".to_string()),
            root_seed: Some("top-secret-seed".to_string()),
            client_credentials_path: Some("/tmp/secret-cc".to_string()),
            ..BackendConfig::default()
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("top-secret-blob"));
        assert!(!debug.contains("top-secret-seed"));
        assert!(!debug.contains("/tmp/secret-cc"));
    }
}
