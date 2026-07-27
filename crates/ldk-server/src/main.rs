mod backend;
mod error;
mod settings;

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
use tokio::signal;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing_subscriber::EnvFilter;

use crate::backend::{Config as BackendConfig, LdkServerBackend};
use crate::settings::Config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg = Config::load()?;

    let cert_pem = fs::read(&cfg.backend.tls_cert_path).with_context(|| {
        format!(
            "failed to read LDK Server TLS certificate {}",
            cfg.backend.tls_cert_path
        )
    })?;

    let backend_cfg = BackendConfig {
        address: cfg.backend.address.clone(),
        api_key: cfg.backend.api_key.clone(),
        cert_pem,
        fee_reserve: FeeReserve {
            min_fee_reserve: Amount::from(cfg.backend.fee_reserve_min_sat),
            percent_fee_reserve: cfg.backend.fee_reserve_percent,
        },
        max_payment_scan_pages: cfg.backend.max_payment_scan_pages,
    };
    let backend = Arc::new(LdkServerBackend::new(backend_cfg)?);

    let socket_addr = SocketAddr::new(
        cfg.address
            .parse::<IpAddr>()
            .with_context(|| format!("invalid listen address {}", cfg.address))?,
        cfg.port,
    );

    let scheme = if cfg.tls_enable { "https" } else { "http" };
    tracing::info!(
        "Starting LDK Server payment processor on {}://{}:{} (node at {})",
        scheme,
        cfg.address,
        cfg.port,
        cfg.backend.address
    );

    let payment_processor = PaymentProcessorService::new(backend, cfg.address.as_str(), cfg.port)?;
    let service = CdkPaymentProcessorServer::with_interceptor(
        payment_processor,
        create_version_check_interceptor(
            cdk_common::grpc::VERSION_HEADER,
            cdk_common::PAYMENT_PROCESSOR_PROTOCOL_VERSION,
        ),
    );

    let server = grpc_server_builder(&cfg)?
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
        self_check(&cfg.address, cfg.port).await?;
    } else {
        tracing::info!("TLS enabled: skipping insecure loopback self-check");
    }

    match serve_task.await {
        Ok(Ok(())) => tracing::info!("Server stopped gracefully"),
        Ok(Err(e)) => return Err(e).context("gRPC server failed"),
        Err(e) => return Err(e).context("gRPC server task panicked"),
    }
    Ok(())
}

/// Verify our own gRPC service answers GetSettings over loopback.
async fn self_check(addr: &str, port: u16) -> Result<()> {
    // cdk-payment-processor 0.17.3 client does not prepend a scheme.
    let endpoint = format!("http://{addr}");
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
            },
        }
    }
    anyhow::bail!(
        "self-check failed: gRPC service on port {port} did not answer GetSettings; \
         refusing to run while not actually serving"
    );
}

fn grpc_server_builder(cfg: &Config) -> Result<Server> {
    let server = Server::builder();

    if !cfg.tls_enable {
        tracing::warn!("TLS is disabled; starting an insecure gRPC server");
        return Ok(server);
    }

    let certificate = fs::read(&cfg.tls_cert_path)
        .with_context(|| format!("failed to read TLS certificate {}", cfg.tls_cert_path))?;
    let private_key = fs::read(&cfg.tls_key_path)
        .with_context(|| format!("failed to read TLS private key {}", cfg.tls_key_path))?;
    let identity = Identity::from_pem(certificate, private_key);
    let tls_config = ServerTlsConfig::new().identity(identity);

    tracing::info!(certificate = %cfg.tls_cert_path, "TLS is enabled");

    server
        .tls_config(tls_config)
        .context("failed to configure gRPC server TLS")
}

/// Wait for shutdown signal (SIGTERM or SIGINT).
async fn shutdown_signal() -> Result<()> {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
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
