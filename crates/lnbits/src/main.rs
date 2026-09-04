use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::{Context, Result};
use cdk_common::grpc::create_version_check_interceptor;
use cdk_common::payment::MintPayment;
use cdk_payment_processor::{
    CdkPaymentProcessorServer, PaymentProcessorServer as PaymentProcessorService,
};
use cdk_payment_processor_lnbits::backend::LnbitsBackend;
use cdk_payment_processor_lnbits::settings::Config;
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

    let config = Config::from_env()?;
    let socket_addr = SocketAddr::new(
        config
            .address
            .parse::<IpAddr>()
            .with_context(|| format!("invalid server address `{}`", config.address))?,
        config.port,
    );
    let mut server_builder = grpc_server_builder(&config, socket_addr)?;

    tracing::info!("Connecting to the LNbits wallet");
    let backend = Arc::new(LnbitsBackend::new(config.lnbits).await?);

    let scheme = if config.tls_enable { "https" } else { "http" };
    tracing::info!(
        "Starting LNbits payment processor on {}://{}:{}",
        scheme,
        config.address,
        config.port
    );

    let payment_processor =
        PaymentProcessorService::new(backend.clone(), config.address.as_str(), config.port)?;
    let service = CdkPaymentProcessorServer::with_interceptor(
        payment_processor,
        create_version_check_interceptor(
            cdk_common::grpc::VERSION_HEADER,
            cdk_common::PAYMENT_PROCESSOR_PROTOCOL_VERSION,
        ),
    );

    let server_result = server_builder
        .add_service(service)
        .serve_with_shutdown(socket_addr, async {
            match shutdown_signal().await {
                Ok(()) => tracing::info!("Shutdown signal received, stopping server"),
                Err(error) => tracing::error!("Error waiting for shutdown signal: {error}"),
            }
        })
        .await;

    if let Err(error) = backend.stop().await {
        tracing::error!("Could not stop the LNbits backend cleanly: {error}");
    }
    server_result?;
    tracing::info!("Server stopped gracefully");
    Ok(())
}

fn grpc_server_builder(config: &Config, socket_addr: SocketAddr) -> Result<Server> {
    let server = Server::builder();

    if !config.tls_enable {
        anyhow::ensure!(
            config.allow_insecure,
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

    let certificate = fs::read(&config.tls_cert_path)
        .with_context(|| format!("failed to read TLS certificate `{}`", config.tls_cert_path))?;
    let private_key = fs::read(&config.tls_key_path)
        .with_context(|| format!("failed to read TLS private key `{}`", config.tls_key_path))?;
    let client_ca = fs::read(&config.tls_client_ca_path).with_context(|| {
        format!(
            "failed to read TLS client CA certificate `{}`",
            config.tls_client_ca_path
        )
    })?;
    let tls_config = ServerTlsConfig::new()
        .identity(Identity::from_pem(certificate, private_key))
        .client_ca_root(Certificate::from_pem(client_ca));

    tracing::info!(
        certificate = %config.tls_cert_path,
        private_key = %config.tls_key_path,
        client_ca = %config.tls_client_ca_path,
        "Mutual TLS is enabled"
    );
    server
        .tls_config(tls_config)
        .context("failed to configure gRPC server TLS")
}

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
            .expect("explicit insecure mode should allow a container-compatible bind");
    }
}
