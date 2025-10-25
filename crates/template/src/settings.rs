use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

/// Backend-specific configuration
///
/// Add fields specific to your Lightning backend implementation here.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendConfig {
    // TODO: Add your backend-specific configuration fields here
    // Examples for different backends:
    // For Blink:
    // pub api_url: Option<String>,
    // pub api_key: Option<String>,
    // pub wallet_id: Option<String>,
    //
    // For LND:
    // pub host: Option<String>,
    // pub macaroon_path: Option<String>,
    // pub tls_cert_path: Option<String>,
    //
    // For Core Lightning:
    // pub socket_path: Option<String>,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {}
    }
}

/// Main configuration structure
///
/// Loads configuration from config.toml and environment variables.
/// Environment variables take precedence over file configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// Backend type identifier (e.g., "blink", "lnd", "cln", "mock")
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
            backend_type: "mock".to_string(),
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
    ///
    /// # TODO
    /// Add environment variable loading for your backend-specific configuration
    ///
    /// # Example
    /// ```rust,ignore
    /// if let Ok(v) = std::env::var("API_URL") {
    ///     cfg.api_url = v;
    /// }
    /// if let Ok(v) = std::env::var("API_KEY") {
    ///     cfg.api_key = v;
    /// }
    /// ```
    pub fn load() -> Self {
        // 1) Start with defaults + config.toml only if it exists
        let base: Config = Default::default();
        let mut fig = Figment::from(Serialized::defaults(base));
        if std::path::Path::new("config.toml").exists() {
            fig = fig.merge(Toml::file("config.toml"));
        }
        let mut cfg: Config = fig.extract().unwrap_or_default();

        // 2) Overlay environment variables explicitly
        // TODO: Add your backend-specific environment variable loading here

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
