use std::future::Future;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use cdk_common::amount::Amount;
use cdk_common::bitcoin::hashes::{sha256, Hash};
use cdk_common::payment::{
    Bolt11IncomingPaymentOptions, Bolt11OutgoingPaymentOptions, Event, IncomingPaymentOptions,
    MintPayment, OutgoingPaymentOptions, PaymentIdentifier,
};
use cdk_common::util::hex;
use cdk_common::{Bolt11Invoice, CurrencyUnit, MeltQuoteState, QuoteId};
use cdk_payment_processor::PaymentProcessorClient;
use cdk_payment_processor_lnbits::backend::LnbitsBackend;
use cdk_payment_processor_lnbits::settings::BackendConfig;
use futures::StreamExt;
use reqwest::{Client, RequestBuilder};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use tokio::process::{Child, Command};
use tokio::time::Instant;

const DEFAULT_LNBITS_IMAGE: &str =
    "lnbits/lnbits:v1.6.0@sha256:dceb42f8a9bedcb868d78e21abde14cf38d76b52bf0cb73c3d87a70af8d693f5";
const DEFAULT_BITCOIND_IMAGE: &str =
    "boltz/bitcoin-core:25.0@sha256:50b230837cf633d028706991b3fcfe6470790831ca1283d12f5a9d84138a2b62";
const DEFAULT_LND_IMAGE: &str =
    "boltz/lnd:0.19.3-beta@sha256:3987e077cf8448ed25c461ade38d77f212b995baab4a449897cdfe2965f46d47";
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const NETWORK_TIMEOUT: Duration = Duration::from_secs(60);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(180);
const BITCOIN_RPC_USER: &str = "regtest";
const BITCOIN_RPC_PASSWORD: &str = "regtest";

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs()
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos()
}

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read ephemeral port")
        .port()
}

async fn eventually<T, F, Fut>(description: &str, timeout: Duration, mut check: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    loop {
        match check().await {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(error) => last_error = Some(error),
        }
        if Instant::now() >= deadline {
            if let Some(error) = last_error {
                bail!("timed out waiting for {description}; last error: {error:#}");
            }
            bail!("timed out waiting for {description}");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[derive(Clone, Deserialize)]
struct WalletCredentials {
    adminkey: String,
    inkey: String,
}

#[derive(Deserialize)]
struct LndInvoiceResponse {
    payment_request: String,
}

#[derive(Deserialize)]
struct LndPaymentResponse {
    payment_hash: String,
    payment_preimage: String,
    status: String,
}

#[derive(Clone)]
struct LnbitsApi {
    client: Client,
    base_url: String,
}

impl LnbitsApi {
    fn new(port: u16) -> Self {
        Self {
            client: Client::new(),
            base_url: format!("http://127.0.0.1:{port}/"),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path.trim_start_matches('/'))
    }

    async fn json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        operation: &str,
    ) -> Result<T> {
        let response = request
            .send()
            .await
            .with_context(|| format!("send LNbits {operation} request"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .with_context(|| format!("read LNbits {operation} response"))?;
        if !status.is_success() {
            bail!("LNbits {operation} failed with HTTP {status}: {body}");
        }
        serde_json::from_str(&body).with_context(|| format!("decode LNbits {operation} response"))
    }

    async fn create_account(&self, name: &str) -> Result<WalletCredentials> {
        self.json(
            self.client
                .post(self.url("api/v1/account"))
                .json(&json!({ "name": name })),
            "account creation",
        )
        .await
    }
}

async fn checked_output(command: &mut Command, operation: &str) -> Result<String> {
    let output = command
        .output()
        .await
        .with_context(|| format!("run {operation}"))?;
    if !output.status.success() {
        bail!(
            "{operation} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).with_context(|| format!("decode {operation} output"))
}

fn json_u64(value: &serde_json::Value, field: &str) -> Option<u64> {
    value
        .get(field)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

#[derive(Clone)]
struct LndNode {
    container_name: String,
}

impl LndNode {
    async fn command(&self, args: &[&str], operation: &str) -> Result<String> {
        let mut command = Command::new("docker");
        command
            .arg("exec")
            .arg(&self.container_name)
            .arg("lncli")
            .arg("--network=regtest");
        command.args(args);
        checked_output(&mut command, operation).await
    }

    async fn json(&self, args: &[&str], operation: &str) -> Result<serde_json::Value> {
        let output = self.command(args, operation).await?;
        serde_json::from_str(&output).with_context(|| format!("decode {operation} response"))
    }

    async fn identity_key(&self) -> Result<String> {
        let info = self.json(&["getinfo"], "LND getinfo").await?;
        info.get("identity_pubkey")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .context("LND getinfo omitted identity_pubkey")
    }

    async fn new_address(&self) -> Result<String> {
        let response = self
            .json(&["newaddress", "p2wkh"], "LND newaddress")
            .await?;
        response
            .get("address")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .context("LND newaddress omitted address")
    }

    async fn create_invoice(&self, amount_sat: u64, memo: &str) -> Result<Bolt11Invoice> {
        let amount_arg = format!("--amt={amount_sat}");
        let memo_arg = format!("--memo={memo}");
        let response = self
            .command(&["addinvoice", &amount_arg, &memo_arg], "LND addinvoice")
            .await?;
        let response: LndInvoiceResponse =
            serde_json::from_str(&response).context("decode LND addinvoice response")?;
        Bolt11Invoice::from_str(&response.payment_request).context("parse LND BOLT11 invoice")
    }

    async fn pay_invoice(&self, invoice: &str) -> Result<LndPaymentResponse> {
        let response = self
            .command(
                &["payinvoice", "--force", "--json", invoice],
                "LND BOLT11 payment",
            )
            .await?;
        let response: LndPaymentResponse =
            serde_json::from_str(&response).context("decode LND payment response")?;
        anyhow::ensure!(
            response.status.eq_ignore_ascii_case("SUCCEEDED"),
            "LND payment ended in status {}",
            response.status
        );
        Ok(response)
    }

    async fn invoice_settled(&self, payment_hash: &str) -> Result<bool> {
        let response = self
            .json(&["lookupinvoice", payment_hash], "LND lookupinvoice")
            .await?;
        Ok(response
            .get("settled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false))
    }
}

struct RegtestEnvironment {
    network_name: String,
    source_volume: String,
    bitcoind_name: String,
    source: LndNode,
    payer: LndNode,
    lnbits_name: String,
    containers: Vec<(String, String)>,
    network_created: bool,
    volume_created: bool,
    log_dir: PathBuf,
    api: LnbitsApi,
    stopped: bool,
}

impl RegtestEnvironment {
    async fn start(run_dir: &Path) -> Result<Self> {
        let suffix = format!("{}-{}", std::process::id(), unique_suffix());
        let log_dir = run_dir.join("logs");
        let data_dir = run_dir.join("lnbits-data");
        std::fs::create_dir_all(&log_dir)?;
        std::fs::create_dir_all(&data_dir)?;
        let data_dir = data_dir.canonicalize()?;

        let mut environment = Self {
            network_name: format!("cdk-lnbits-regtest-net-{suffix}"),
            source_volume: format!("cdk-lnbits-regtest-lnd-{suffix}"),
            bitcoind_name: format!("cdk-lnbits-bitcoind-{suffix}"),
            source: LndNode {
                container_name: format!("cdk-lnbits-source-{suffix}"),
            },
            payer: LndNode {
                container_name: format!("cdk-lnbits-payer-{suffix}"),
            },
            lnbits_name: format!("cdk-lnbits-server-{suffix}"),
            containers: Vec::new(),
            network_created: false,
            volume_created: false,
            log_dir,
            api: LnbitsApi::new(pick_port()),
            stopped: false,
        };
        environment.create_network_and_volume().await?;
        environment.start_bitcoind().await?;
        environment.prepare_chain().await?;
        environment.start_lnd_nodes().await?;
        environment.prepare_channel().await?;
        environment.start_lnbits(&data_dir).await?;
        environment.wait_for_lnbits().await?;
        Ok(environment)
    }

    async fn create_network_and_volume(&mut self) -> Result<()> {
        checked_output(
            Command::new("docker")
                .arg("network")
                .arg("create")
                .arg(&self.network_name),
            "Docker network creation",
        )
        .await?;
        self.network_created = true;

        checked_output(
            Command::new("docker")
                .arg("volume")
                .arg("create")
                .arg(&self.source_volume),
            "Docker volume creation",
        )
        .await?;
        self.volume_created = true;
        Ok(())
    }

    async fn start_bitcoind(&mut self) -> Result<()> {
        let image = std::env::var("LNBITS_REGTEST_BITCOIND_IMAGE")
            .unwrap_or_else(|_| DEFAULT_BITCOIND_IMAGE.to_string());
        checked_output(
            Command::new("docker")
                .arg("run")
                .arg("--detach")
                .arg("--rm")
                .arg("--name")
                .arg(&self.bitcoind_name)
                .arg("--network")
                .arg(&self.network_name)
                .arg(image)
                .arg("-regtest")
                .arg("-server=1")
                .arg("-printtoconsole=1")
                .arg(format!("-rpcuser={BITCOIN_RPC_USER}"))
                .arg(format!("-rpcpassword={BITCOIN_RPC_PASSWORD}"))
                .arg("-rpcallowip=0.0.0.0/0")
                .arg("-rpcbind=0.0.0.0")
                .arg("-fallbackfee=0.00000253")
                .arg("-txindex=1")
                .arg("-zmqpubrawtx=tcp://0.0.0.0:29000")
                .arg("-zmqpubrawblock=tcp://0.0.0.0:29001"),
            "bitcoind container startup",
        )
        .await?;
        self.containers
            .push((self.bitcoind_name.clone(), "bitcoind".to_string()));

        eventually("bitcoind RPC readiness", STARTUP_TIMEOUT, || async {
            self.bitcoin_cli(&["getblockchaininfo"])
                .await
                .map(|_| Some(()))
        })
        .await
    }

    async fn bitcoin_cli(&self, args: &[&str]) -> Result<String> {
        let mut command = Command::new("docker");
        command
            .arg("exec")
            .arg(&self.bitcoind_name)
            .arg("bitcoin-cli")
            .arg("-regtest")
            .arg(format!("-rpcuser={BITCOIN_RPC_USER}"))
            .arg(format!("-rpcpassword={BITCOIN_RPC_PASSWORD}"));
        command.args(args);
        checked_output(&mut command, "bitcoin-cli").await
    }

    async fn prepare_chain(&self) -> Result<()> {
        self.bitcoin_cli(&["createwallet", "regtest"])
            .await
            .context("create bitcoind wallet")?;
        let address = self
            .bitcoin_cli(&["getnewaddress"])
            .await
            .context("create mining address")?;
        self.bitcoin_cli(&["generatetoaddress", "101", address.trim()])
            .await
            .context("mine initial regtest blocks")?;
        Ok(())
    }

    async fn start_lnd_nodes(&mut self) -> Result<()> {
        let source = self.source.clone();
        let payer = self.payer.clone();
        self.start_lnd(&source, Some(self.source_volume.clone()))
            .await?;
        self.start_lnd(&payer, None).await?;

        for (label, node) in [("source LND", source), ("payer LND", payer)] {
            eventually(&format!("{label} readiness"), STARTUP_TIMEOUT, || async {
                let info = node.json(&["getinfo"], "LND getinfo").await?;
                Ok(info
                    .get("synced_to_chain")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
                    .then_some(()))
            })
            .await?;
        }
        Ok(())
    }

    async fn start_lnd(&mut self, node: &LndNode, volume: Option<String>) -> Result<()> {
        let image = std::env::var("LNBITS_REGTEST_LND_IMAGE")
            .unwrap_or_else(|_| DEFAULT_LND_IMAGE.to_string());
        let mut command = Command::new("docker");
        command
            .arg("run")
            .arg("--detach")
            .arg("--rm")
            .arg("--name")
            .arg(&node.container_name)
            .arg("--hostname")
            .arg(&node.container_name)
            .arg("--network")
            .arg(&self.network_name);
        if let Some(volume) = volume {
            command.arg("--volume").arg(format!("{volume}:/root/.lnd"));
        }
        command
            .arg(image)
            .arg("--listen=0.0.0.0:9735")
            .arg("--rpclisten=0.0.0.0:10009")
            .arg("--restlisten=0.0.0.0:8081")
            .arg("--bitcoin.active")
            .arg("--bitcoin.regtest")
            .arg("--bitcoin.node=bitcoind")
            .arg(format!("--bitcoind.rpchost={}:18443", self.bitcoind_name))
            .arg(format!("--bitcoind.rpcuser={BITCOIN_RPC_USER}"))
            .arg(format!("--bitcoind.rpcpass={BITCOIN_RPC_PASSWORD}"))
            .arg(format!(
                "--bitcoind.zmqpubrawtx=tcp://{}:29000",
                self.bitcoind_name
            ))
            .arg(format!(
                "--bitcoind.zmqpubrawblock=tcp://{}:29001",
                self.bitcoind_name
            ))
            .arg("--noseedbackup")
            .arg("--protocol.wumbo-channels")
            .arg(format!("--tlsextradomain={}", node.container_name));
        checked_output(&mut command, "LND container startup").await?;
        let role = if node.container_name == self.source.container_name {
            "lnd-source"
        } else {
            "lnd-payer"
        };
        self.containers
            .push((node.container_name.clone(), role.to_string()));
        Ok(())
    }

    async fn prepare_channel(&self) -> Result<()> {
        let payer_address = self.payer.new_address().await?;
        let source_address = self.source.new_address().await?;
        self.bitcoin_cli(&["sendtoaddress", &payer_address, "0.02"])
            .await
            .context("fund payer LND")?;
        self.bitcoin_cli(&["sendtoaddress", &source_address, "0.01"])
            .await
            .context("fund source LND")?;
        let mining_address = self.bitcoin_cli(&["getnewaddress"]).await?;
        self.bitcoin_cli(&["generatetoaddress", "6", mining_address.trim()])
            .await
            .context("confirm LND funding")?;

        eventually("payer LND confirmed balance", NETWORK_TIMEOUT, || async {
            let balance = self
                .payer
                .json(&["walletbalance"], "payer LND walletbalance")
                .await?;
            Ok(json_u64(&balance, "confirmed_balance")
                .is_some_and(|balance| balance >= 2_000_000)
                .then_some(()))
        })
        .await?;

        let source_key = self.source.identity_key().await?;
        let peer = format!("{source_key}@{}:9735", self.source.container_name);
        eventually("LND peer connection", STARTUP_TIMEOUT, || async {
            self.payer
                .command(&["connect", &peer], "connect LND peers")
                .await
                .map(|_| Some(()))
        })
        .await?;
        self.payer
            .command(
                &["openchannel", &source_key, "1000000", "400000"],
                "open funded LND channel",
            )
            .await?;
        let mining_address = self.bitcoin_cli(&["getnewaddress"]).await?;
        self.bitcoin_cli(&["generatetoaddress", "6", mining_address.trim()])
            .await
            .context("confirm LND channel")?;

        for (label, node) in [
            ("payer LND active channel", &self.payer),
            ("source LND active channel", &self.source),
        ] {
            eventually(label, NETWORK_TIMEOUT, || async {
                let info = node.json(&["getinfo"], "LND getinfo").await?;
                Ok(json_u64(&info, "num_active_channels")
                    .is_some_and(|count| count > 0)
                    .then_some(()))
            })
            .await?;
        }
        Ok(())
    }

    async fn start_lnbits(&mut self, data_dir: &Path) -> Result<()> {
        let image = std::env::var("LNBITS_REGTEST_IMAGE")
            .unwrap_or_else(|_| DEFAULT_LNBITS_IMAGE.to_string());
        let port = self
            .api
            .base_url
            .trim_end_matches('/')
            .rsplit(':')
            .next()
            .context("LNbits URL omitted port")?;
        let lnd_endpoint = format!("https://{}:8081/", self.source.container_name);
        checked_output(
            Command::new("docker")
                .arg("run")
                .arg("--detach")
                .arg("--rm")
                .arg("--name")
                .arg(&self.lnbits_name)
                .arg("--network")
                .arg(&self.network_name)
                .arg("--publish")
                .arg(format!("127.0.0.1:{port}:5000"))
                .arg("--volume")
                .arg(format!("{}:/app/data", data_dir.display()))
                .arg("--volume")
                .arg(format!("{}:/app/lnd:ro", self.source_volume))
                .arg("--env")
                .arg("LNBITS_DATA_FOLDER=/app/data")
                .arg("--env")
                .arg("LNBITS_BACKEND_WALLET_CLASS=LndRestWallet")
                .arg("--env")
                .arg(format!("LND_REST_ENDPOINT={lnd_endpoint}"))
                .arg("--env")
                .arg("LND_REST_CERT=/app/lnd/tls.cert")
                .arg("--env")
                .arg("LND_REST_MACAROON=/app/lnd/data/chain/bitcoin/regtest/admin.macaroon")
                .arg("--env")
                .arg("LNBITS_ADMIN_UI=false")
                .arg("--env")
                .arg("AUTH_HTTPS_ONLY=false")
                .arg("--env")
                .arg("LNBITS_EXTENSIONS_DEACTIVATE_ALL=true")
                .arg("--env")
                .arg("LNBITS_EXTENSIONS_DEFAULT_INSTALL=")
                .arg(image),
            "LNbits container startup",
        )
        .await?;
        self.containers
            .push((self.lnbits_name.clone(), "lnbits".to_string()));
        Ok(())
    }

    async fn wait_for_lnbits(&self) -> Result<()> {
        eventually("LNbits readiness", STARTUP_TIMEOUT, || async {
            let response = self
                .api
                .client
                .get(self.api.url("api/v1/health"))
                .send()
                .await?;
            Ok(response.status().is_success().then_some(()))
        })
        .await
        .with_context(|| format!("LNbits logs are in {}", self.log_dir.display()))
    }

    async fn capture_logs(&self) {
        for (container, role) in &self.containers {
            if let Ok(output) = Command::new("docker")
                .arg("logs")
                .arg(container)
                .output()
                .await
            {
                let _ = std::fs::write(self.log_dir.join(format!("{role}.out.log")), output.stdout);
                let _ = std::fs::write(self.log_dir.join(format!("{role}.err.log")), output.stderr);
            }
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.capture_logs().await;
        for (container, _) in self.containers.iter().rev() {
            let _ = Command::new("docker")
                .arg("rm")
                .arg("--force")
                .arg(container)
                .output()
                .await;
        }
        if self.volume_created {
            let _ = Command::new("docker")
                .arg("volume")
                .arg("rm")
                .arg("--force")
                .arg(&self.source_volume)
                .output()
                .await;
        }
        if self.network_created {
            let _ = Command::new("docker")
                .arg("network")
                .arg("rm")
                .arg(&self.network_name)
                .output()
                .await;
        }
        self.stopped = true;
        Ok(())
    }
}

impl Drop for RegtestEnvironment {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        for (container, role) in &self.containers {
            if let Ok(output) = StdCommand::new("docker")
                .arg("logs")
                .arg(container)
                .output()
            {
                let _ = std::fs::write(self.log_dir.join(format!("{role}.out.log")), output.stdout);
                let _ = std::fs::write(self.log_dir.join(format!("{role}.err.log")), output.stderr);
            }
        }
        for (container, _) in self.containers.iter().rev() {
            let _ = StdCommand::new("docker")
                .arg("rm")
                .arg("--force")
                .arg(container)
                .output();
        }
        if self.volume_created {
            let _ = StdCommand::new("docker")
                .arg("volume")
                .arg("rm")
                .arg("--force")
                .arg(&self.source_volume)
                .output();
        }
        if self.network_created {
            let _ = StdCommand::new("docker")
                .arg("network")
                .arg("rm")
                .arg(&self.network_name)
                .output();
        }
    }
}

fn backend_config(api: &LnbitsApi, wallet: &WalletCredentials) -> BackendConfig {
    BackendConfig {
        admin_api_key: wallet.adminkey.clone(),
        invoice_api_key: wallet.inkey.clone(),
        api_url: api.base_url.clone(),
        fee_reserve_min_sat: 2,
        fee_reserve_percent: 0.02,
    }
}

fn bolt11_options(invoice: Bolt11Invoice, max_fee_sat: u64) -> OutgoingPaymentOptions {
    OutgoingPaymentOptions::Bolt11(Box::new(Bolt11OutgoingPaymentOptions {
        bolt11: invoice,
        max_fee_amount: Some(Amount::new(max_fee_sat, CurrencyUnit::Sat)),
        timeout_secs: Some(30),
        melt_options: None,
        quote_id: QuoteId::new(),
    }))
}

async fn settings_scenario(backend: &LnbitsBackend) -> Result<()> {
    backend.start().await.context("backend start")?;
    let settings = backend.get_settings().await?;
    assert_eq!(settings.unit, "sat");
    let bolt11 = settings.bolt11.context("BOLT11 settings missing")?;
    assert!(!bolt11.mpp);
    assert!(!bolt11.amountless);
    assert!(bolt11.invoice_description);
    assert!(settings.bolt12.is_none());
    assert!(settings.onchain.is_none());
    assert!(settings.custom.is_empty());
    Ok(())
}

async fn receive_scenario(payer: &LndNode, backend: &LnbitsBackend) -> Result<()> {
    let amount_sat = 12_000_u64;
    let mut stream = backend.wait_payment_event().await?;
    let response = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("LNbits regtest receive".to_string()),
                amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 300),
            },
        ))
        .await?;
    let invoice = Bolt11Invoice::from_str(&response.request)?;
    let payment_hash = invoice.payment_hash().to_byte_array();
    let payment_hash_hex = hex::encode(payment_hash);
    assert_eq!(invoice.amount_milli_satoshis(), Some(amount_sat * 1_000));
    assert_eq!(invoice.description().to_string(), "LNbits regtest receive");
    assert_eq!(
        response.request_lookup_id,
        PaymentIdentifier::PaymentHash(payment_hash)
    );
    assert!(response.expiry.is_some_and(|expiry| expiry > unix_now()));
    assert!(backend
        .check_incoming_payment_status(&response.request_lookup_id)
        .await?
        .is_empty());

    let paid = payer.pay_invoice(&response.request).await?;
    assert_eq!(paid.payment_hash, payment_hash_hex);
    let preimage = hex::decode(&paid.payment_preimage).context("decode payer payment preimage")?;
    assert_eq!(sha256::Hash::hash(&preimage).to_byte_array(), payment_hash);

    let event = tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
        .await
        .context("receive event timed out")?
        .context("receive event stream ended")?;
    let Event::PaymentReceived(received) = event else {
        bail!("unexpected event while waiting for LNbits receive");
    };
    assert_eq!(received.payment_identifier, response.request_lookup_id);
    assert_eq!(received.payment_amount.clone().to_u64(), amount_sat * 1_000);
    assert_eq!(received.payment_id, payment_hash_hex);

    let payments = backend
        .check_incoming_payment_status(&response.request_lookup_id)
        .await?;
    assert_eq!(payments.len(), 1);
    assert_eq!(
        payments[0].payment_amount.clone().to_u64(),
        amount_sat * 1_000
    );
    assert_eq!(payments[0].payment_id, payment_hash_hex);

    drop(stream);
    eventually(
        "event stream marked inactive",
        Duration::from_secs(5),
        || async { Ok((!backend.is_payment_event_stream_active()).then_some(())) },
    )
    .await?;
    Ok(())
}

async fn send_scenario(payer: &LndNode, backend: &LnbitsBackend) -> Result<()> {
    let amount_sat = 10_000_u64;
    let invoice = payer
        .create_invoice(amount_sat, "LNbits regtest send")
        .await?;
    let payment_hash = invoice.payment_hash().to_byte_array();
    let payment_hash_hex = hex::encode(payment_hash);

    let quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), u64::MAX),
        )
        .await?;
    assert_eq!(quote.amount.clone().to_u64(), amount_sat);
    assert_eq!(quote.fee.clone().to_u64(), 200);
    assert_eq!(quote.state, MeltQuoteState::Unpaid);
    assert_eq!(
        quote.request_lookup_id,
        Some(PaymentIdentifier::PaymentHash(payment_hash))
    );

    let paid = backend
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(invoice, quote.fee.to_u64()),
        )
        .await?;
    assert_eq!(paid.status, MeltQuoteState::Paid);
    assert_eq!(
        paid.payment_lookup_id,
        PaymentIdentifier::PaymentHash(payment_hash)
    );
    assert_eq!(paid.total_spent.clone().to_u64(), amount_sat);
    let proof = paid
        .payment_proof
        .context("paid send must include a preimage")?;
    let proof = hex::decode(&proof).context("decode payment preimage")?;
    assert_eq!(sha256::Hash::hash(&proof).to_byte_array(), payment_hash);

    let checked = backend
        .check_outgoing_payment(&PaymentIdentifier::PaymentHash(payment_hash))
        .await?;
    assert_eq!(checked.status, MeltQuoteState::Paid);
    assert_eq!(checked.total_spent.to_u64(), amount_sat);
    assert!(checked.payment_proof.is_some());

    eventually("payer invoice settlement", NETWORK_TIMEOUT, || async {
        Ok(payer
            .invoice_settled(&payment_hash_hex)
            .await?
            .then_some(()))
    })
    .await?;
    Ok(())
}

struct ProcessorProcess {
    child: Child,
    port: u16,
}

impl ProcessorProcess {
    async fn spawn(
        api: &LnbitsApi,
        wallet: &WalletCredentials,
        port: u16,
        logs: &Path,
        attempt: u8,
    ) -> Result<Self> {
        let stdout =
            std::fs::File::create(logs.join(format!("processor-{port}-{attempt}.out.log")))?;
        let stderr =
            std::fs::File::create(logs.join(format!("processor-{port}-{attempt}.err.log")))?;
        let child = Command::new(env!("CARGO_BIN_EXE_cdk-payment-processor-lnbits"))
            .env("SERVER_ADDRESS", "127.0.0.1")
            .env("SERVER_PORT", port.to_string())
            .env("ALLOW_INSECURE", "true")
            .env("LNBITS_ADMIN_API_KEY", &wallet.adminkey)
            .env("LNBITS_INVOICE_API_KEY", &wallet.inkey)
            .env("LNBITS_API_URL", &api.base_url)
            .env("RUST_LOG", "debug")
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .context("spawn cdk-payment-processor-lnbits")?;
        Ok(Self { child, port })
    }

    async fn client(&mut self) -> Result<PaymentProcessorClient> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match PaymentProcessorClient::new("127.0.0.1", self.port, None).await {
                Ok(client) => return Ok(client),
                Err(_) => {
                    if let Some(status) = self.child.try_wait()? {
                        bail!("processor exited early with status {status}");
                    }
                    if Instant::now() >= deadline {
                        bail!("processor never became ready");
                    }
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill().await?;
        }
        let _ = self.child.wait().await;
        Ok(())
    }
}

impl Drop for ProcessorProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

async fn processor_restart_scenario(
    api: &LnbitsApi,
    processor_wallet: &WalletCredentials,
    payer: &LndNode,
    logs: &Path,
) -> Result<()> {
    let port = pick_port();
    let mut process = ProcessorProcess::spawn(api, processor_wallet, port, logs, 0).await?;
    let client = process.client().await?;

    let settings = client.get_settings().await?;
    assert_eq!(settings.unit, "sat");
    assert!(settings
        .bolt11
        .is_some_and(|bolt11| !bolt11.amountless && !bolt11.mpp));
    assert!(settings.bolt12.is_none());
    assert!(settings.onchain.is_none());

    let incoming = client
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("gRPC restart receive".to_string()),
                amount: Amount::new(8_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 300),
            },
        ))
        .await?;
    process.stop().await?;
    payer.pay_invoice(&incoming.request).await?;

    process = ProcessorProcess::spawn(api, processor_wallet, port, logs, 1).await?;
    let client = process.client().await?;
    let payments = eventually(
        "incoming payment visible after processor restart",
        NETWORK_TIMEOUT,
        || async {
            let payments = client
                .check_incoming_payment_status(&incoming.request_lookup_id)
                .await
                .map_err(anyhow::Error::from)?;
            Ok((!payments.is_empty()).then_some(payments))
        },
    )
    .await?;
    assert_eq!(payments.len(), 1);
    assert_eq!(payments[0].payment_amount.clone().to_u64(), 8_000_000);

    let invoice = payer.create_invoice(6_000, "gRPC LNbits send").await?;
    let payment_hash = invoice.payment_hash().to_byte_array();
    let payment_hash_hex = hex::encode(payment_hash);
    let quote = client
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), u64::MAX),
        )
        .await?;
    let paid = client
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(invoice, quote.fee.to_u64()),
        )
        .await?;
    assert_eq!(paid.status, MeltQuoteState::Paid);
    assert_eq!(paid.total_spent.to_u64(), 6_000);
    assert!(paid.payment_proof.is_some());
    let checked = client
        .check_outgoing_payment(&PaymentIdentifier::PaymentHash(payment_hash))
        .await?;
    assert_eq!(checked.status, MeltQuoteState::Paid);

    eventually("gRPC payer invoice settlement", NETWORK_TIMEOUT, || async {
        Ok(payer
            .invoice_settled(&payment_hash_hex)
            .await?
            .then_some(()))
    })
    .await?;

    process.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and pinned regtest images; run `just test-regtest`"]
async fn lnbits_regtest_suite() -> Result<()> {
    let root = std::env::var("TEST_DIRECTORY")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/lnbits-regtest")
        });
    let run_dir = root.join(format!("run-{}", unix_now()));
    std::fs::create_dir_all(run_dir.join("logs"))?;

    eprintln!("starting Bitcoin Core, two LND nodes, and LNbits...");
    let mut environment = RegtestEnvironment::start(&run_dir).await?;
    let processor_wallet = environment.api.create_account("processor-wallet").await?;
    eprintln!("LNbits ready with a funded bidirectional regtest channel");

    let backend = LnbitsBackend::new(backend_config(&environment.api, &processor_wallet))
        .await
        .context("connect processor backend")?;
    eprintln!("running direct backend scenarios");
    settings_scenario(&backend)
        .await
        .context("settings scenario")?;
    receive_scenario(&environment.payer, &backend)
        .await
        .context("BOLT11 receive scenario")?;
    send_scenario(&environment.payer, &backend)
        .await
        .context("BOLT11 send scenario")?;
    backend.stop().await?;
    eprintln!("direct backend scenarios complete");

    eprintln!("running processor restart scenario");
    processor_restart_scenario(
        &environment.api,
        &processor_wallet,
        &environment.payer,
        &run_dir.join("logs"),
    )
    .await
    .context("black-box processor restart scenario")?;
    eprintln!("processor restart scenario complete");

    environment.stop().await?;
    eprintln!("regtest artifacts kept in {}", run_dir.display());
    Ok(())
}
