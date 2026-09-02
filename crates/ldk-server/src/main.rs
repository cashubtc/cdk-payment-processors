use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use cdk_common::amount::Amount;
use cdk_common::common::FeeReserve;
use cdk_common::grpc::create_version_check_interceptor;
use cdk_common::payment::MintPayment;
use cdk_payment_processor::{
    CdkPaymentProcessorServer, PaymentProcessorClient,
    PaymentProcessorServer as PaymentProcessorService,
};
use cdk_payment_processor_ldk_server::backend::{Config as BackendConfig, LdkServerBackend};
use cdk_payment_processor_ldk_server::settings::Config;
use tokio::signal;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing_subscriber::EnvFilter;

const INSECURE_GUIDANCE: &str = "configure mTLS with tls_enable = true and \
    tls_cert_path/tls_key_path/tls_client_ca_path, or set allow_insecure = true to accept \
    cleartext traffic";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg = Config::load()?;
    let socket_addr = SocketAddr::new(
        cfg.address
            .parse::<IpAddr>()
            .with_context(|| format!("invalid listen address {}", cfg.address))?,
        cfg.port,
    );
    let mut server_builder = grpc_server_builder(&cfg, socket_addr)?;

    let cert_pem = fs::read(&cfg.ldk.tls_cert_path).with_context(|| {
        format!(
            "failed to read LDK Server TLS certificate {}",
            cfg.ldk.tls_cert_path
        )
    })?;

    let backend_cfg = BackendConfig {
        address: cfg.ldk.address.clone(),
        api_key: cfg.ldk.api_key.clone(),
        cert_pem,
        fee_reserve: FeeReserve {
            min_fee_reserve: Amount::from(cfg.ldk.fee_reserve_min_sat),
            percent_fee_reserve: cfg.ldk.fee_reserve_percent,
        },
        max_payment_scan_pages: cfg.ldk.max_payment_scan_pages,
        advertised_methods: cfg.ldk.advertised_methods()?,
    };
    let backend = Arc::new(LdkServerBackend::new(backend_cfg)?);

    let scheme = if cfg.tls_enable { "https" } else { "http" };
    tracing::info!(
        "Starting LDK Server payment processor on {}://{}:{} (node at {})",
        scheme,
        cfg.address,
        cfg.port,
        cfg.ldk.address
    );

    let payment_processor = PaymentProcessorService::new(backend, cfg.address.as_str(), cfg.port)?;
    let service = CdkPaymentProcessorServer::with_interceptor(
        payment_processor,
        create_version_check_interceptor(
            cdk_common::grpc::VERSION_HEADER,
            cdk_common::PAYMENT_PROCESSOR_PROTOCOL_VERSION,
        ),
    );

    let server = server_builder
        .add_service(service)
        .serve_with_shutdown(socket_addr, async {
            match shutdown_signal().await {
                Ok(()) => tracing::info!("Shutdown signal received, stopping server..."),
                Err(error) => tracing::error!("Error waiting for shutdown signal: {error}"),
            }
        });

    let serve_task = tokio::spawn(server);

    // Fail fast instead of serving nothing if the gRPC endpoint is not really
    // reachable (e.g. a conflicting listener raced us to the port).
    if !cfg.tls_enable {
        self_check(socket_addr).await?;
    } else {
        tracing::info!("TLS enabled: skipping plaintext self-check");
    }

    match serve_task.await {
        Ok(Ok(())) => tracing::info!("Server stopped gracefully"),
        Ok(Err(e)) => return Err(e).context("gRPC server failed"),
        Err(e) => return Err(e).context("gRPC server task panicked"),
    }
    Ok(())
}

/// Verify our own gRPC service answers GetSettings from the local host.
async fn self_check(socket_addr: SocketAddr) -> Result<()> {
    // cdk-payment-processor 0.18 chooses the scheme from the TLS configuration.
    let endpoint = self_check_endpoint(socket_addr);
    let port = socket_addr.port();
    for attempt in 1..=10u8 {
        let attempt_result: Result<()> = async {
            let client = tokio::time::timeout(
                Duration::from_secs(2),
                PaymentProcessorClient::new(&endpoint, port, None),
            )
            .await
            .map_err(|_| anyhow::anyhow!("connect timed out"))??;
            let settings = tokio::time::timeout(Duration::from_secs(2), client.get_settings())
                .await
                .map_err(|_| anyhow::anyhow!("get_settings timed out"))??;
            tracing::info!(
                "Self-check OK: unit={} bolt11={} bolt12={}",
                settings.unit,
                settings.bolt11.is_some(),
                settings.bolt12.is_some()
            );
            Ok(())
        }
        .await;
        match attempt_result {
            Ok(()) => return Ok(()),
            Err(e) => {
                tracing::warn!("Self-check attempt {attempt}/10 failed: {e}");
                if attempt < 10 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
    anyhow::bail!(
        "self-check failed: gRPC service on port {port} did not answer GetSettings; \
         refusing to run while not actually serving"
    );
}

fn self_check_endpoint(socket_addr: SocketAddr) -> String {
    let check_ip = match socket_addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => std::net::Ipv4Addr::LOCALHOST.into(),
        IpAddr::V6(ip) if ip.is_unspecified() => std::net::Ipv6Addr::LOCALHOST.into(),
        ip => ip,
    };
    match check_ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

fn grpc_server_builder(cfg: &Config, socket_addr: SocketAddr) -> Result<Server> {
    let server = Server::builder();

    if !cfg.tls_enable {
        anyhow::ensure!(
            cfg.allow_insecure,
            "payment processor TLS is required: {INSECURE_GUIDANCE}"
        );
        if socket_addr.ip().is_loopback() {
            tracing::warn!(
                bind_address = %socket_addr,
                "TLS is disabled; starting an explicitly allowed insecure gRPC server"
            );
        } else {
            tracing::warn!(
                bind_address = %socket_addr,
                "TLS is disabled on a non-loopback bind; cleartext payment RPCs may be exposed to the network"
            );
        }
        return Ok(server);
    }

    let certificate = fs::read(&cfg.tls_cert_path)
        .with_context(|| format!("failed to read TLS certificate `{}`", cfg.tls_cert_path))?;
    let private_key = fs::read(&cfg.tls_key_path)
        .with_context(|| format!("failed to read TLS private key `{}`", cfg.tls_key_path))?;
    let client_ca = fs::read(&cfg.tls_client_ca_path).with_context(|| {
        format!(
            "failed to read TLS client CA certificate `{}`",
            cfg.tls_client_ca_path
        )
    })?;
    let identity = Identity::from_pem(certificate, private_key);
    let tls_config = ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(Certificate::from_pem(client_ca));

    tracing::info!(
        certificate = %cfg.tls_cert_path,
        private_key = %cfg.tls_key_path,
        client_ca = %cfg.tls_client_ca_path,
        "mutual TLS is enabled"
    );

    server
        .tls_config(tls_config)
        .context("failed to configure gRPC server TLS")
}

/// Wait for shutdown signal (SIGTERM or SIGINT).
async fn shutdown_signal() -> Result<()> {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insecure_config(address: &str, allow_insecure: bool) -> Config {
        Config {
            address: address.to_owned(),
            allow_insecure,
            ..Config::default()
        }
    }

    #[test]
    fn plaintext_without_explicit_opt_in_is_rejected() {
        let config = insecure_config("127.0.0.1", false);
        let error = match grpc_server_builder(&config, "127.0.0.1:50051".parse().unwrap()) {
            Ok(_) => panic!("plaintext without opt-in must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains(INSECURE_GUIDANCE));
    }

    #[test]
    fn explicitly_insecure_loopback_is_allowed() {
        let config = insecure_config("127.0.0.1", true);

        grpc_server_builder(&config, "127.0.0.1:50051".parse().unwrap())
            .expect("explicit loopback development mode should be allowed");
    }

    #[test]
    fn explicitly_insecure_non_loopback_is_allowed() {
        let config = insecure_config("0.0.0.0", true);

        grpc_server_builder(&config, "0.0.0.0:50051".parse().unwrap())
            .expect("explicit insecure mode should allow a Docker-compatible bind");
    }

    #[test]
    fn self_check_uses_loopback_for_unspecified_docker_bind() {
        assert_eq!(
            self_check_endpoint("0.0.0.0:50051".parse().unwrap()),
            "127.0.0.1"
        );
        assert_eq!(self_check_endpoint("[::]:50051".parse().unwrap()), "[::1]");
    }
}
