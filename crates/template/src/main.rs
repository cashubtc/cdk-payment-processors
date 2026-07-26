mod backend;
mod settings;

use crate::backend::TemplateBackend;
use crate::settings::Config;
use anyhow::{Context, Result};
use cdk_common::grpc::create_version_check_interceptor;
use cdk_payment_processor::{
    CdkPaymentProcessorServer, PaymentProcessorServer as PaymentProcessorService,
};
use std::{
    fs,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use tokio::signal;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let cfg = Config::from_env()?;
    let mut server_builder = grpc_server_builder(&cfg)?;
    let socket_addr = SocketAddr::new(
        cfg.address
            .parse::<IpAddr>()
            .with_context(|| format!("invalid server address `{}`", cfg.address))?,
        cfg.port,
    );

    // TODO: Initialize your Lightning backend here
    // For now, we use the template backend which will panic with todo!() on any method call
    let backend = Arc::new(TemplateBackend::new()?);

    // Optional: Test the connection
    // backend.test_connection().await?;

    let scheme = if cfg.tls_enable { "https" } else { "http" };
    tracing::info!(
        "Starting CDK Payment Processor server on {}://{}:{}",
        scheme,
        cfg.address,
        cfg.port
    );

    let payment_processor = PaymentProcessorService::new(backend, cfg.address.as_str(), cfg.port)?;
    let service = CdkPaymentProcessorServer::with_interceptor(
        payment_processor,
        create_version_check_interceptor(
            cdk_common::grpc::VERSION_HEADER,
            cdk_common::PAYMENT_PROCESSOR_PROTOCOL_VERSION,
        ),
    );

    server_builder
        .add_service(service)
        .serve_with_shutdown(socket_addr, async {
            match shutdown_signal().await {
                Ok(()) => tracing::info!("Shutdown signal received, stopping server..."),
                Err(error) => tracing::error!("Error waiting for shutdown signal: {error}"),
            }
        })
        .await?;
    tracing::info!("Server stopped gracefully");
    Ok(())
}

fn grpc_server_builder(cfg: &Config) -> Result<Server> {
    let server = Server::builder();

    if !cfg.tls_enable {
        tracing::warn!("TLS is disabled; starting an insecure gRPC server");
        return Ok(server);
    }

    let certificate = fs::read(&cfg.tls_cert_path)
        .with_context(|| format!("failed to read TLS certificate `{}`", cfg.tls_cert_path))?;
    let private_key = fs::read(&cfg.tls_key_path)
        .with_context(|| format!("failed to read TLS private key `{}`", cfg.tls_key_path))?;
    let identity = Identity::from_pem(certificate, private_key);
    let tls_config = ServerTlsConfig::new().identity(identity);

    tracing::info!(
        certificate = %cfg.tls_cert_path,
        private_key = %cfg.tls_key_path,
        "TLS is enabled"
    );

    server
        .tls_config(tls_config)
        .context("failed to configure gRPC server TLS")
}

/// Wait for shutdown signal (SIGTERM or SIGINT)
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
