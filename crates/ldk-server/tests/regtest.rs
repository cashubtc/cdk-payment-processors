use std::future::Future;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use bitcoin::hashes::{sha256, Hash};
use cdk::amount::SplitTarget;
use cdk::mint::{MintBuilder, MintMeltLimits};
use cdk::nuts::{
    CurrencyUnit as CashuUnit, MeltQuoteState as CashuMeltState, MintQuoteState, PaymentMethod,
    ProofsMethods,
};
use cdk::types::QuoteTTL;
use cdk::wallet::{MeltOutcome, Wallet};
use cdk_common::amount::Amount;
use cdk_common::common::FeeReserve;
use cdk_common::nuts::MeltOptions;
use cdk_common::payment::{
    Bolt11IncomingPaymentOptions, Bolt11OutgoingPaymentOptions, Bolt12IncomingPaymentOptions,
    CreateIncomingPaymentResponse, Event, IncomingPaymentOptions, MakePaymentResponse, MintPayment,
    OutgoingPaymentOptions, PaymentIdentifier, PaymentQuoteResponse, SettingsResponse,
    WaitPaymentResponse,
};
use cdk_common::util::hex;
use cdk_common::{CurrencyUnit, MeltQuoteState, QuoteId};
use cdk_payment_processor::PaymentProcessorClient;
use cdk_payment_processor_ldk_server::backend::{Config as BackendConfig, LdkServerBackend};
use corepc_node::Node;
use futures::{Stream, StreamExt};
use ldk_server_client::client::{EventStream, LdkServerClient};
use ldk_server_client::ldk_server_grpc::api::{
    Bolt11FailForHashRequest, Bolt11ReceiveForHashRequest, Bolt11ReceiveRequest, Bolt11SendRequest,
    Bolt12SendRequest, GetBalancesRequest, GetNodeInfoRequest, ListChannelsRequest,
    OnchainReceiveRequest, OpenChannelRequest,
};
use ldk_server_client::ldk_server_grpc::events::event_envelope;
use ldk_server_client::ldk_server_grpc::types::{
    bolt11_invoice_description, payment_kind, Bolt11InvoiceDescription,
};
use lightning_invoice::Bolt11Invoice;
use tokio::process::{Child as TokioChild, Command as TokioCommand};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

// Keep in sync with the `ldk-server-client` rev pinned in Cargo.toml so the
// daemon under test always speaks the protocol the client was generated from.
const LDK_SERVER_REPO: &str = "https://github.com/lightningdevkit/ldk-server";
const LDK_SERVER_REV: &str = "50fe7523be3529d86bfee0dfc35df9a52aca7310";
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const NETWORK_TIMEOUT: Duration = Duration::from_secs(120);
const FEE_RESERVE_PADDING_SAT: u64 = 50;
const CHANNEL_SATS: u64 = 200_000;
const CHANNEL_PUSH_MSAT: u64 = 80_000_000;
static NEXT_PROCESS_LOG_ID: AtomicU64 = AtomicU64::new(0);

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs()
}

fn pick_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
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

fn rpc<T, E: std::fmt::Display>(result: Result<T, E>) -> Result<T> {
    result.map_err(|error| anyhow::anyhow!("ldk-server RPC failed: {error}"))
}

async fn wait_for_file(path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        if Instant::now() >= deadline {
            bail!("timed out waiting for file {}", path.display());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    Ok(())
}

fn target_dir() -> PathBuf {
    std::env::current_exe()
        .expect("current exe")
        .ancestors()
        .nth(3)
        .expect("exe sits inside the cargo target directory")
        .to_path_buf()
}

struct Bitcoind {
    node: Node,
}

impl Bitcoind {
    fn start() -> Result<Self> {
        let node = match std::env::var("BITCOIND_EXE") {
            Ok(exe) => Node::new(&exe).context("start bitcoind from BITCOIND_EXE")?,
            Err(_) => Node::from_downloaded().context("download and start bitcoind")?,
        };
        let address = node.client.new_address().context("bitcoind new address")?;
        node.client
            .generate_to_address(101, &address)
            .context("mine initial regtest blocks")?;
        Ok(Self { node })
    }

    fn block_count(&self) -> Result<u64> {
        Ok(self
            .node
            .client
            .get_block_count()
            .context("getblockcount")?
            .0)
    }

    fn mine(&self, count: u64) -> Result<()> {
        let address = self.node.client.new_address().context("new address")?;
        self.node
            .client
            .generate_to_address(count as usize, &address)
            .context("generate blocks")?;
        Ok(())
    }

    fn fund_address(&self, address: &str, btc: f64) -> Result<()> {
        use corepc_node::client::bitcoin::address::NetworkUnchecked;
        use corepc_node::client::bitcoin::Amount;

        let parsed: corepc_node::client::bitcoin::Address<NetworkUnchecked> =
            address.parse().context("parse on-chain address")?;
        let parsed = parsed.assume_checked();
        let amount = Amount::from_btc(btc).context("parse btc amount")?;
        self.node
            .client
            .send_to_address(&parsed, amount)
            .context("send to address")?;
        self.mine(1)
    }

    fn rpc_details(&self) -> Result<(String, u16, String, String)> {
        let rpc_url = self.node.rpc_url();
        let rpc_address = rpc_url.strip_prefix("http://").unwrap_or(&rpc_url);
        let (host, port) = rpc_address
            .split_once(':')
            .context("unexpected bitcoind rpc url")?;
        let cookie = std::fs::read_to_string(&self.node.params.cookie_file)
            .context("read bitcoind cookie file")?;
        let (user, password) = cookie.split_once(':').context("unexpected cookie format")?;
        Ok((
            host.to_string(),
            port.parse().context("parse bitcoind rpc port")?,
            user.to_string(),
            password.to_string(),
        ))
    }
}

/// Locates or produces the pinned `ldk-server` daemon binary.
///
/// An explicit `LDK_SERVER_EXE` wins; otherwise the upstream repository is
/// cloned at the revision matching `ldk-server-client` in Cargo.toml and built
/// into a separate target directory (cached between runs).
fn ensure_ldk_server_binary() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("LDK_SERVER_EXE") {
        let path = PathBuf::from(path);
        anyhow::ensure!(
            path.is_file(),
            "LDK_SERVER_EXE={} is not a file",
            path.display()
        );
        return Ok(path);
    }

    let src_dir = target_dir().join("ldk-server-src");
    std::fs::create_dir_all(&src_dir)?;

    if src_dir.join(".git").exists() {
        // Refreshing the pinned commit is best effort; a checkout that already
        // contains it keeps working offline.
        let _ = Command::new("git")
            .arg("-C")
            .arg(&src_dir)
            .args(["fetch", "--quiet", "--depth", "1", "origin", LDK_SERVER_REV])
            .status();
    } else {
        eprintln!("Cloning {LDK_SERVER_REPO}...");
        let status = Command::new("git")
            .args(["clone", "--quiet", "--filter=blob:none", LDK_SERVER_REPO])
            .arg(&src_dir)
            .status()
            .context(
                "git clone failed; install git or point LDK_SERVER_EXE at \
                 an existing ldk-server binary",
            )?;
        anyhow::ensure!(status.success(), "cloning {LDK_SERVER_REPO} failed");
    }

    let checkout = Command::new("git")
        .arg("-C")
        .arg(&src_dir)
        .args(["checkout", "--quiet", "--force", LDK_SERVER_REV])
        .status()
        .context("git checkout of the pinned ldk-server revision")?;
    anyhow::ensure!(checkout.success(), "git checkout {LDK_SERVER_REV} failed");

    // A dedicated target directory avoids deadlocking on the outer cargo's
    // build-directory lock while tests are running.
    let build_target = target_dir().join("ldk-server-build");
    let bin_path = build_target.join("debug").join("ldk-server");
    if !bin_path.exists() {
        eprintln!("Building the pinned ldk-server daemon (cached afterwards)...");
    }
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = Command::new(cargo)
        .args(["build", "--bin", "ldk-server"])
        .current_dir(&src_dir)
        .env("CARGO_TARGET_DIR", &build_target)
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .status()
        .context("cargo build of the ldk-server daemon")?;
    anyhow::ensure!(
        status.success(),
        "cargo build of the ldk-server daemon failed"
    );
    anyhow::ensure!(
        bin_path.is_file(),
        "expected daemon binary at {}",
        bin_path.display()
    );
    Ok(bin_path)
}

struct LdkNode {
    child: Child,
    grpc_port: u16,
    p2p_port: u16,
    api_key: String,
    tls_cert_pem: Vec<u8>,
    tls_cert_path: PathBuf,
    node_id: String,
    client: LdkServerClient,
}

impl LdkNode {
    async fn start(
        bitcoind: &Bitcoind,
        alias: &str,
        storage_dir: PathBuf,
        binary: &Path,
    ) -> Result<Self> {
        std::fs::create_dir_all(&storage_dir)?;
        let grpc_port = pick_port();
        let p2p_port = pick_port();
        let (rpc_host, rpc_port, rpc_user, rpc_password) = bitcoind.rpc_details()?;
        let config = format!(
            "[node]\n\
             network = \"regtest\"\n\
             listening_addresses = [\"127.0.0.1:{p2p_port}\"]\n\
             grpc_service_address = \"127.0.0.1:{grpc_port}\"\n\
             alias = \"{alias}\"\n\
             \n\
             [storage.disk]\n\
             dir_path = \"{}\"\n\
             \n\
             [bitcoind]\n\
             rpc_address = \"{rpc_host}:{rpc_port}\"\n\
             rpc_user = \"{rpc_user}\"\n\
             rpc_password = \"{rpc_password}\"\n",
            storage_dir.display(),
        );
        let config_path = storage_dir.join("config.toml");
        std::fs::write(&config_path, config)?;

        let stdout_log = storage_dir.join("stdout.log");
        let stderr_log = storage_dir.join("stderr.log");
        let stdout = std::fs::File::create(&stdout_log)?;
        let stderr = std::fs::File::create(&stderr_log)?;
        let mut child = Command::new(binary)
            .arg(&config_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .with_context(|| format!("spawn ldk-server daemon at {}", binary.display()))?;

        let api_key_path = storage_dir.join("regtest").join("api_key");
        let tls_cert_path = storage_dir.join("tls.crt");
        let credentials = tokio::try_join!(
            wait_for_file(&api_key_path, Duration::from_secs(60)),
            wait_for_file(&tls_cert_path, Duration::from_secs(60)),
        );
        if let Err(error) = credentials {
            let _ = child.kill();
            let _ = child.wait();
            bail!("daemon {alias} did not produce credentials: {error:#}");
        }

        let api_key_bytes = std::fs::read(&api_key_path)?;
        anyhow::ensure!(!api_key_bytes.is_empty(), "empty ldk-server api key file");
        let api_key = hex::encode(api_key_bytes);
        let tls_cert_pem = std::fs::read(&tls_cert_path)?;

        let client = LdkServerClient::new(
            format!("127.0.0.1:{grpc_port}"),
            api_key.clone(),
            &tls_cert_pem,
        )
        .map_err(anyhow::Error::msg)
        .context("build ldk-server client")?;

        // Debug-profile daemons can take a while to sync the pre-mined chain.
        let deadline = Instant::now() + Duration::from_secs(180);
        let ready = loop {
            if let Ok(info) = client.get_node_info(GetNodeInfoRequest {}).await {
                break Some(info);
            }
            if child.try_wait()?.is_some() {
                bail!("daemon exited before becoming ready");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                let tail = std::fs::read_to_string(&stderr_log).unwrap_or_default();
                let tail = tail.lines().rev().take(40).collect::<Vec<_>>().join("\n");
                bail!("daemon {alias} never became ready\n--- stderr tail ---\n{tail}");
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        };
        let info = ready.context("daemon readiness result missing")?;

        Ok(Self {
            child,
            grpc_port,
            p2p_port,
            api_key,
            tls_cert_pem,
            tls_cert_path,
            node_id: info.node_id,
            client,
        })
    }

    async fn mine_and_sync(&self, bitcoind: &Bitcoind, count: u64) -> Result<()> {
        bitcoind.mine(count)?;
        let expected = bitcoind.block_count()? as u32;
        eventually("chain sync", NETWORK_TIMEOUT, || async {
            let info = rpc(self.client.get_node_info(GetNodeInfoRequest {}).await)?;
            Ok((info
                .current_best_block
                .map(|block| block.height >= expected)
                == Some(true))
            .then_some(()))
        })
        .await
    }
}

impl Drop for LdkNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn open_channel(bitcoind: &Bitcoind, from: &LdkNode, to: &LdkNode) -> Result<()> {
    let addr_from = rpc(from.client.onchain_receive(OnchainReceiveRequest {}).await)?.address;
    let addr_to = rpc(to.client.onchain_receive(OnchainReceiveRequest {}).await)?.address;
    bitcoind.fund_address(&addr_from, 1.0)?;
    bitcoind.fund_address(&addr_to, 0.5)?;
    from.mine_and_sync(bitcoind, 6).await?;
    to.mine_and_sync(bitcoind, 6).await?;

    for node in [from, to] {
        eventually("on-chain balance", NETWORK_TIMEOUT, || async {
            let balances = rpc(node.client.get_balances(GetBalancesRequest {}).await)?;
            Ok((balances.spendable_onchain_balance_sats > 0).then_some(()))
        })
        .await?;
    }

    let opened = rpc(from
        .client
        .open_channel(OpenChannelRequest {
            node_pubkey: to.node_id.clone(),
            address: format!("127.0.0.1:{}", to.p2p_port),
            channel_amount_sats: CHANNEL_SATS,
            push_to_counterparty_msat: Some(CHANNEL_PUSH_MSAT),
            channel_config: None,
            announce_channel: false,
            disable_counterparty_reserve: false,
        })
        .await)?;
    eprintln!("Channel {} opening", opened.user_channel_id);

    from.mine_and_sync(bitcoind, 6).await?;
    to.mine_and_sync(bitcoind, 6).await?;

    eventually("usable channel", NETWORK_TIMEOUT, || async {
        let channels = rpc(from.client.list_channels(ListChannelsRequest {}).await)?;
        if channels.channels.iter().any(|channel| channel.is_usable) {
            return Ok(Some(()));
        }
        bitcoind.mine(1)?;
        Ok(None)
    })
    .await?;
    Ok(())
}

fn backend_config(node: &LdkNode) -> BackendConfig {
    BackendConfig {
        address: format!("127.0.0.1:{}", node.grpc_port),
        api_key: node.api_key.clone(),
        cert_pem: node.tls_cert_pem.clone(),
        fee_reserve: FeeReserve {
            min_fee_reserve: Amount::from(2_u64),
            percent_fee_reserve: 0.01,
        },
        max_payment_scan_pages: 32,
    }
}

fn bolt11_options(
    invoice: Bolt11Invoice,
    quote_id: QuoteId,
    max_fee_sat: u64,
    melt_options: Option<MeltOptions>,
) -> OutgoingPaymentOptions {
    OutgoingPaymentOptions::Bolt11(Box::new(Bolt11OutgoingPaymentOptions {
        bolt11: invoice,
        max_fee_amount: Some(Amount::new(max_fee_sat, CurrencyUnit::Sat)),
        timeout_secs: Some(30),
        melt_options,
        quote_id,
    }))
}

async fn payer_invoice(
    payer: &LdkNode,
    amount_msat: Option<u64>,
    description: &str,
) -> Result<Bolt11Invoice> {
    let response = rpc(payer
        .client
        .bolt11_receive(Bolt11ReceiveRequest {
            amount_msat,
            description: Some(Bolt11InvoiceDescription {
                kind: Some(bolt11_invoice_description::Kind::Direct(
                    description.to_string(),
                )),
            }),
            expiry_secs: 3_600,
        })
        .await)?;
    Bolt11Invoice::from_str(&response.invoice).context("parse payer invoice")
}

/// The daemon records payments before they settle, so `make_payment` can
/// legitimately return `Pending`; finality arrives through status checks.
async fn await_terminal(
    backend: &LdkServerBackend,
    identifier: &PaymentIdentifier,
) -> Result<MakePaymentResponse> {
    eventually(
        "outgoing payment terminal state",
        NETWORK_TIMEOUT,
        || async {
            let response = backend
                .check_outgoing_payment(identifier)
                .await
                .map_err(anyhow::Error::from)?;
            Ok((response.status != MeltQuoteState::Pending).then_some(response))
        },
    )
    .await
}

struct ProcessorProcess {
    child: TokioChild,
    port: u16,
}

impl ProcessorProcess {
    async fn spawn(node: &LdkNode, port: u16, log_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(log_dir)?;
        let log_id = NEXT_PROCESS_LOG_ID.fetch_add(1, Ordering::Relaxed);
        let stdout =
            std::fs::File::create(log_dir.join(format!("processor-{port}-{log_id}.out.log")))?;
        let stderr =
            std::fs::File::create(log_dir.join(format!("processor-{port}-{log_id}.err.log")))?;
        let child = TokioCommand::new(env!("CARGO_BIN_EXE_cdk-payment-processor-ldk-server"))
            .env("SERVER_ADDRESS", "127.0.0.1")
            .env("SERVER_PORT", port.to_string())
            .env("ALLOW_INSECURE", "true")
            .env("LDK_ADDRESS", format!("127.0.0.1:{}", node.grpc_port))
            .env("LDK_API_KEY", &node.api_key)
            .env("LDK_TLS_CERT_PATH", &node.tls_cert_path)
            .env("RUST_LOG", "debug")
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .context("spawn cdk-payment-processor-ldk-server")?;
        Ok(Self { child, port })
    }

    async fn client(&self) -> Result<PaymentProcessorClient> {
        eventually("processor readiness", Duration::from_secs(30), || async {
            match PaymentProcessorClient::new("127.0.0.1", self.port, None).await {
                Ok(client) => Ok(Some(client)),
                Err(_) => Ok(None),
            }
        })
        .await
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

#[derive(Clone)]
struct FeeReservePaddingProcessor {
    inner: PaymentProcessorClient,
    padding_sat: u64,
}

#[async_trait::async_trait]
impl MintPayment for FeeReservePaddingProcessor {
    type Err = cdk_common::payment::Error;

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        self.inner.get_settings().await
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        self.inner.create_incoming_payment_request(options).await
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        let pad_fee = matches!(&options, OutgoingPaymentOptions::Bolt11(_));
        let mut quote = self.inner.get_payment_quote(unit, options).await?;
        if pad_fee {
            quote.fee = Amount::new(
                quote.fee.to_u64().saturating_add(self.padding_sat),
                unit.clone(),
            );
        }
        Ok(quote)
    }

    async fn make_payment(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err> {
        self.inner.make_payment(unit, options).await
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<std::pin::Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        self.inner.wait_payment_event().await
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.inner.is_payment_event_stream_active()
    }

    fn cancel_payment_event_stream(&self) {
        self.inner.cancel_payment_event_stream();
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        self.inner
            .check_incoming_payment_status(payment_identifier)
            .await
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        self.inner.check_outgoing_payment(payment_identifier).await
    }
}

struct MintHttpServer {
    mint: Arc<cdk::Mint>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: JoinHandle<Result<(), std::io::Error>>,
}

impl MintHttpServer {
    async fn start(
        processor: PaymentProcessorClient,
        database_path: &Path,
        seed: &[u8; 64],
        port: u16,
    ) -> Result<Self> {
        let database = Arc::new(
            cdk_sqlite::MintSqliteDatabase::new(database_path.to_path_buf())
                .await
                .context("create mint database")?,
        );
        let mut builder = MintBuilder::new(database.clone())
            .with_name("LDK Server regtest mint".to_string())
            .with_description("LDK Server payment processor regtest".to_string())
            .with_urls(vec![format!("http://127.0.0.1:{port}")]);
        builder
            .add_payment_processor(
                CashuUnit::Sat,
                PaymentMethod::BOLT11,
                MintMeltLimits::new(1, 2_000_000),
                Arc::new(FeeReservePaddingProcessor {
                    inner: processor,
                    padding_sat: FEE_RESERVE_PADDING_SAT,
                }),
            )
            .await?;
        let mint = Arc::new(builder.build_with_seed(database, seed).await?);
        mint.set_quote_ttl(QuoteTTL::new(120, 120)).await?;
        mint.start().await?;

        let router = cdk_axum::create_mint_router(mint.clone(), vec!["bolt11".to_string()]).await?;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Ok(Self {
            mint,
            shutdown: Some(shutdown_tx),
            handle,
        })
    }

    async fn stop(mut self) -> Result<()> {
        self.mint.stop().await?;
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.handle.await??;
        Ok(())
    }
}

async fn settings_scenario(backend: &LdkServerBackend) -> Result<()> {
    backend.start().await.context("backend start")?;
    let settings = backend.get_settings().await?;
    assert_eq!(settings.unit, "msat");
    let bolt11 = settings.bolt11.context("BOLT11 settings missing")?;
    assert!(!bolt11.mpp);
    assert!(bolt11.amountless);
    assert!(bolt11.invoice_description);
    let bolt12 = settings.bolt12.context("BOLT12 settings missing")?;
    assert!(bolt12.amountless);
    assert!(settings.onchain.is_none());
    assert!(settings.custom.is_empty());
    Ok(())
}

async fn bolt11_receive_scenario(payer: &LdkNode, backend: &LdkServerBackend) -> Result<()> {
    let amount_sat = 12_000_u64;
    let mut stream = backend.wait_payment_event().await?;
    let response = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("ldk-server regtest receive".to_string()),
                amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 300),
            },
        ))
        .await?;
    let invoice = Bolt11Invoice::from_str(&response.request)?;
    let payment_hash: [u8; 32] = *invoice.payment_hash().as_ref();
    assert_eq!(invoice.amount_milli_satoshis(), Some(amount_sat * 1_000));
    assert_eq!(
        invoice.description().to_string(),
        "ldk-server regtest receive"
    );
    assert_eq!(
        response.request_lookup_id,
        PaymentIdentifier::PaymentHash(payment_hash)
    );
    assert!(response.expiry.is_some_and(|expiry| expiry > unix_now()));
    assert!(backend
        .check_incoming_payment_status(&response.request_lookup_id)
        .await?
        .is_empty());

    rpc(payer
        .client
        .bolt11_send(Bolt11SendRequest {
            invoice: response.request.clone(),
            amount_msat: None,
            route_parameters: None,
        })
        .await)
    .context("payer bolt11 send")?;

    let event = tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
        .await
        .context("receive event timed out")?
        .context("receive event stream ended")?;
    let Event::PaymentReceived(received) = event else {
        bail!("unexpected event while waiting for lightning receive")
    };
    assert_eq!(received.payment_identifier, response.request_lookup_id);
    assert_eq!(received.payment_amount.to_u64(), amount_sat * 1_000);
    assert_eq!(received.payment_id, hex::encode(payment_hash));

    let payments = backend
        .check_incoming_payment_status(&response.request_lookup_id)
        .await?;
    assert_eq!(payments.len(), 1);
    assert_eq!(
        payments[0].payment_amount.clone().to_u64(),
        amount_sat * 1_000
    );
    assert_eq!(payments[0].payment_id, hex::encode(payment_hash));

    backend.cancel_payment_event_stream();
    drop(stream);
    Ok(())
}

async fn bolt12_receive_scenario(payer: &LdkNode, backend: &LdkServerBackend) -> Result<()> {
    let amount_sat = 8_000_u64;
    let response = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt12(Box::new(
            Bolt12IncomingPaymentOptions {
                description: Some("ldk-server regtest offer".to_string()),
                amount: Some(Amount::new(amount_sat, CurrencyUnit::Sat)),
                unix_expiry: None,
            },
        )))
        .await?;
    assert!(
        matches!(response.request_lookup_id, PaymentIdentifier::OfferId(_)),
        "bolt12 receive must identify by offer id"
    );
    assert!(response.request.starts_with("lno"), "offer must be bech32");

    rpc(payer
        .client
        .bolt12_send(Bolt12SendRequest {
            offer: response.request.clone(),
            amount_msat: Some(amount_sat * 1_000),
            quantity: None,
            payer_note: None,
            route_parameters: None,
        })
        .await)
    .context("payer bolt12 send")?;

    let payments = eventually("bolt12 incoming payment", NETWORK_TIMEOUT, || async {
        let payments = backend
            .check_incoming_payment_status(&response.request_lookup_id)
            .await
            .map_err(anyhow::Error::from)?;
        Ok((!payments.is_empty()).then_some(payments))
    })
    .await?;
    assert_eq!(payments.len(), 1);
    // LDK may report a small overpayment on claimed BOLT12 payments.
    assert!(
        payments[0].payment_amount.clone().to_u64() >= amount_sat * 1_000,
        "bolt12 payment underpaid"
    );
    Ok(())
}

async fn bolt11_send_scenario(payer: &LdkNode, backend: &LdkServerBackend) -> Result<()> {
    let invoice = payer_invoice(payer, Some(9_000_000), "melt me").await?;
    let payment_hash: [u8; 32] = *invoice.payment_hash().as_ref();

    let quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), QuoteId::new(), u64::MAX, None),
        )
        .await?;
    assert_eq!(quote.amount.to_u64(), 9_000);
    assert_eq!(quote.state, MeltQuoteState::Unpaid);
    assert_eq!(
        quote.request_lookup_id,
        Some(PaymentIdentifier::PaymentHash(payment_hash))
    );
    let quoted_fee = quote.fee.to_u64();
    assert_eq!(quoted_fee, 90);

    let mismatched = bolt11_options(
        invoice.clone(),
        QuoteId::new(),
        u64::MAX,
        Some(MeltOptions::new_amountless(5_000_000_u64)),
    );
    assert!(
        backend
            .get_payment_quote(&CurrencyUnit::Sat, mismatched)
            .await
            .is_err(),
        "an amountless option disagreeing with the invoice amount must be rejected"
    );

    let started = backend
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(invoice, QuoteId::new(), quoted_fee, None),
        )
        .await?;
    if started.status == MeltQuoteState::Pending {
        await_terminal(backend, &started.payment_lookup_id).await?;
    }

    // Status checks report totals in msat regardless of the quote unit.
    let by_hash = backend
        .check_outgoing_payment(&PaymentIdentifier::PaymentHash(payment_hash))
        .await?;
    assert_eq!(by_hash.status, MeltQuoteState::Paid);
    let proof = by_hash
        .payment_proof
        .clone()
        .context("paid send must carry a preimage")?;
    assert_eq!(proof.len(), 64);
    let spent_msat = by_hash.total_spent.to_u64();
    assert!(
        spent_msat >= 9_000_000 && spent_msat <= 9_000_000 + quoted_fee * 1_000,
        "total spent {spent_msat} msat outside expected range"
    );

    let by_id = backend
        .check_outgoing_payment(&started.payment_lookup_id)
        .await?;
    assert_eq!(by_id.status, MeltQuoteState::Paid);
    Ok(())
}

async fn amountless_send_scenario(payer: &LdkNode, backend: &LdkServerBackend) -> Result<()> {
    let invoice = payer_invoice(payer, None, "variable amount").await?;
    let melt = MeltOptions::new_amountless(4_000_000_u64);
    let quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), QuoteId::new(), u64::MAX, Some(melt)),
        )
        .await?;
    assert_eq!(quote.amount.to_u64(), 4_000);
    let quoted_fee = quote.fee.to_u64();

    let started = backend
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(invoice, QuoteId::new(), quoted_fee, Some(melt)),
        )
        .await?;
    if started.status == MeltQuoteState::Pending {
        await_terminal(backend, &started.payment_lookup_id).await?;
    }

    let by_hash = backend
        .check_outgoing_payment(&started.payment_lookup_id)
        .await?;
    assert_eq!(by_hash.status, MeltQuoteState::Paid);
    assert!(by_hash.payment_proof.is_some());
    let spent_msat = by_hash.total_spent.to_u64();
    assert!(
        spent_msat >= 4_000_000 && spent_msat <= 4_000_000 + quoted_fee * 1_000,
        "total spent {spent_msat} msat outside expected range"
    );
    Ok(())
}

async fn held_htlc_invoice(payer: &LdkNode, amount_msat: u64, marker: u8) -> Result<Bolt11Invoice> {
    let payment_hash = sha256::Hash::hash(&[marker; 32]);
    let response = rpc(payer
        .client
        .bolt11_receive_for_hash(Bolt11ReceiveForHashRequest {
            amount_msat: Some(amount_msat),
            description: Some(Bolt11InvoiceDescription {
                kind: Some(bolt11_invoice_description::Kind::Direct(format!(
                    "held invoice {marker}"
                ))),
            }),
            expiry_secs: 600,
            payment_hash: hex::encode(payment_hash.as_byte_array()),
        })
        .await)?;
    Bolt11Invoice::from_str(&response.invoice).context("parse held invoice")
}

/// Waits until the hodl invoice holder reports the HTLC as claimable. Failing
/// or claiming earlier races the in-flight HTLC and does nothing.
async fn await_claimable(mut stream: EventStream, payment_hash: [u8; 32]) -> Result<()> {
    let hash_hex = hex::encode(payment_hash);
    let deadline = Instant::now() + NETWORK_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            bail!("timed out waiting for PaymentClaimable on {hash_hex}");
        }
        let message = tokio::time::timeout(Duration::from_secs(30), stream.next_message())
            .await
            .context("claimable event wait timed out")?;
        match message {
            Some(Ok(envelope)) => match envelope.event {
                Some(event_envelope::Event::PaymentClaimable(claimable)) => {
                    if let Some(kind) = claimable.payment.and_then(|payment| payment.kind) {
                        if let Some(payment_kind::Kind::Bolt11(bolt11)) = kind.kind {
                            if bolt11.hash.eq_ignore_ascii_case(&hash_hex) {
                                return Ok(());
                            }
                        }
                    }
                }
                _ => continue,
            },
            Some(Err(error)) => bail!("event stream error: {error}"),
            None => bail!("event stream ended before PaymentClaimable"),
        }
    }
}

async fn pending_then_failed_send_scenario(
    payer: &LdkNode,
    backend: &LdkServerBackend,
) -> Result<()> {
    let invoice = held_htlc_invoice(payer, 7_000_000, 77).await?;
    let payment_hash: [u8; 32] = *invoice.payment_hash().as_ref();

    let quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), QuoteId::new(), u64::MAX, None),
        )
        .await?;
    assert_eq!(quote.amount.to_u64(), 7_000);
    let quoted_fee = quote.fee.to_u64();

    let claimable_stream = rpc(payer.client.subscribe_events().await)?;
    let started = backend
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(invoice, QuoteId::new(), quoted_fee, None),
        )
        .await
        .context("make_payment on held invoice")?;
    assert_eq!(started.status, MeltQuoteState::Pending);
    assert_eq!(started.total_spent.to_u64(), 0);
    assert!(started.payment_proof.is_none());

    // In-flight payments are only reachable through their payment id; the
    // hash-indexed history gains the record once a terminal event lands.
    let held = backend
        .check_outgoing_payment(&started.payment_lookup_id)
        .await
        .context("held status lookup by id")?;
    assert_eq!(held.status, MeltQuoteState::Pending);

    await_claimable(claimable_stream, payment_hash).await?;
    rpc(payer
        .client
        .bolt11_fail_for_hash(Bolt11FailForHashRequest {
            payment_hash: hex::encode(payment_hash),
        })
        .await)
    .context("fail held HTLC")?;

    let hash_identifier = PaymentIdentifier::PaymentHash(payment_hash);
    let failed = eventually(
        "failed outgoing payment status",
        NETWORK_TIMEOUT,
        || async {
            match backend.check_outgoing_payment(&hash_identifier).await {
                Ok(status) => Ok((status.status != MeltQuoteState::Pending).then_some(status)),
                // Tolerate the window before the terminal event is indexed.
                Err(error) if error.to_string().contains("not found") => Ok(None),
                Err(error) => bail!("outgoing status lookup failed: {error:#}"),
            }
        },
    )
    .await?;
    assert_eq!(failed.status, MeltQuoteState::Failed);
    assert_eq!(failed.total_spent.to_u64(), 0);
    assert!(failed.payment_proof.is_none());
    Ok(())
}

async fn expiry_scenarios(payer: &LdkNode, backend: &LdkServerBackend) -> Result<()> {
    let error = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("past expiry".to_string()),
                amount: Amount::new(5_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now().saturating_sub(60)),
            },
        ))
        .await
        .err()
        .context("past-expiry request must fail")?;
    assert!(
        error.to_string().contains("expiry"),
        "unexpected rejection reason: {error}"
    );

    let short_lived = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("short lived receive".to_string()),
                amount: Amount::new(5_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 2),
            },
        ))
        .await?;
    let invoice = Bolt11Invoice::from_str(&short_lived.request)?;
    eventually("invoice expiry", Duration::from_secs(20), || async {
        Ok(invoice.is_expired().then_some(()))
    })
    .await?;

    // Either the payer refuses the expired invoice up front or the attempt
    // fails remotely; either way the processor must never report credit.
    let _sent = payer
        .client
        .bolt11_send(Bolt11SendRequest {
            invoice: short_lived.request.clone(),
            amount_msat: None,
            route_parameters: None,
        })
        .await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            backend
                .check_incoming_payment_status(&short_lived.request_lookup_id)
                .await?
                .is_empty(),
            "an expired invoice must never become mint credit"
        );
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    Ok(())
}

async fn processor_scenario(payer: &LdkNode, node: &LdkNode, logs: &Path) -> Result<()> {
    let port = pick_port();
    let mut process = ProcessorProcess::spawn(node, port, logs).await?;
    let client = process.client().await?;

    let settings = client.get_settings().await?;
    assert_eq!(settings.unit, "msat");
    assert!(settings
        .bolt11
        .is_some_and(|bolt11| bolt11.amountless && !bolt11.mpp));
    assert!(settings.bolt12.is_some());
    assert!(settings.onchain.is_none());

    let response = client
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("gRPC restart receive".to_string()),
                amount: Amount::new(10_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 300),
            },
        ))
        .await?;

    process.stop().await?;
    rpc(payer
        .client
        .bolt11_send(Bolt11SendRequest {
            invoice: response.request.clone(),
            amount_msat: None,
            route_parameters: None,
        })
        .await)
    .context("pay invoice while processor process is down")?;

    process = ProcessorProcess::spawn(node, port, logs).await?;
    let client = process.client().await?;
    let payments = eventually(
        "incoming payment visible after processor restart",
        NETWORK_TIMEOUT,
        || async {
            let payments = client
                .check_incoming_payment_status(&response.request_lookup_id)
                .await
                .map_err(anyhow::Error::from)?;
            Ok((!payments.is_empty()).then_some(payments))
        },
    )
    .await?;
    assert_eq!(payments.len(), 1);
    assert_eq!(payments[0].payment_amount.clone().to_u64(), 10_000 * 1_000);

    let invoice = payer_invoice(payer, Some(6_000_000), "gRPC melt").await?;
    let payment_hash: [u8; 32] = *invoice.payment_hash().as_ref();
    let quote = client
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), QuoteId::new(), u64::MAX, None),
        )
        .await?;
    let started = client
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(invoice, QuoteId::new(), quote.fee.to_u64(), None),
        )
        .await?;
    let paid = if started.status == MeltQuoteState::Pending {
        let identifier = started.payment_lookup_id.clone();
        eventually("gRPC melt terminal state", NETWORK_TIMEOUT, || async {
            let response = client
                .check_outgoing_payment(&identifier)
                .await
                .map_err(anyhow::Error::from)?;
            Ok((response.status != MeltQuoteState::Pending).then_some(response))
        })
        .await?
    } else {
        started
    };
    assert_eq!(paid.status, MeltQuoteState::Paid);
    assert!(paid.payment_proof.is_some());
    let by_hash = client
        .check_outgoing_payment(&PaymentIdentifier::PaymentHash(payment_hash))
        .await?;
    assert_eq!(by_hash.status, MeltQuoteState::Paid);

    process.stop().await?;
    Ok(())
}

async fn cashu_scenario(
    payer: &LdkNode,
    run_dir: &Path,
    mut process: ProcessorProcess,
) -> Result<()> {
    let mint_port = pick_port();
    let mint_url = format!("http://127.0.0.1:{mint_port}");
    let mint_db = run_dir.join("cashu-mint.sqlite");
    let wallet_db = Arc::new(cdk_sqlite::wallet::memory::empty().await?);
    let wallet_seed = [7_u8; 64];
    let mint_seed = [42_u8; 64];

    let mint =
        MintHttpServer::start(process.client().await?, &mint_db, &mint_seed, mint_port).await?;
    let wallet = Wallet::new(&mint_url, CashuUnit::Sat, wallet_db, wallet_seed, None)
        .context("create cashu wallet")?;
    assert_eq!(wallet.total_balance().await?.to_u64(), 0);

    let mint_quote = wallet
        .mint_quote(
            PaymentMethod::BOLT11,
            Some(cdk::Amount::from(18_000)),
            Some("full ldk-server mint".to_string()),
            None,
        )
        .await
        .context("create cashu mint quote")?;
    assert_eq!(mint_quote.state, MintQuoteState::Unpaid);
    rpc(payer
        .client
        .bolt11_send(Bolt11SendRequest {
            invoice: mint_quote.request.clone(),
            amount_msat: None,
            route_parameters: None,
        })
        .await)
    .context("fund cashu mint quote")?;
    let paid_quote = eventually("Cashu mint quote paid", NETWORK_TIMEOUT, || async {
        let quote = wallet.check_mint_quote_status(&mint_quote.id).await?;
        Ok((quote.state == MintQuoteState::Paid).then_some(quote))
    })
    .await?;
    assert_eq!(
        paid_quote.amount.map(|amount| amount.to_u64()),
        Some(18_000)
    );

    let proofs = wallet
        .mint(&mint_quote.id, SplitTarget::default(), None)
        .await
        .context("mint proofs after payment")?;
    assert_eq!(proofs.total_amount()?.to_u64(), 18_000);
    assert_eq!(wallet.total_balance().await?.to_u64(), 18_000);
    assert_eq!(
        wallet.check_mint_quote_status(&mint_quote.id).await?.state,
        MintQuoteState::Issued
    );

    let melt_invoice = payer_invoice(payer, Some(10_000_000), "Cashu melt").await?;
    let melt_quote = wallet
        .melt_quote(PaymentMethod::BOLT11, melt_invoice.to_string(), None, None)
        .await
        .context("cashu melt quote")?;
    assert_eq!(melt_quote.state, CashuMeltState::Unpaid);

    let before = wallet.total_balance().await?.to_u64();
    let finalized = wallet
        .prepare_melt(&melt_quote.id, Default::default())
        .await
        .context("prepare cashu melt")?
        .confirm()
        .await
        .context("confirm cashu melt")?;
    assert_eq!(finalized.state(), CashuMeltState::Paid);
    assert!(finalized.payment_proof().is_some());
    assert_eq!(
        before - wallet.total_balance().await?.to_u64(),
        finalized.amount().to_u64() + finalized.fee_paid().to_u64()
    );
    assert!(finalized.fee_paid().to_u64() < melt_quote.fee_reserve.to_u64());

    let held_hash = sha256::Hash::hash(&[99_u8; 32]);
    let held_response = rpc(payer
        .client
        .bolt11_receive_for_hash(Bolt11ReceiveForHashRequest {
            amount_msat: Some(5_000_000),
            description: Some(Bolt11InvoiceDescription {
                kind: Some(bolt11_invoice_description::Kind::Direct(
                    "Cashu failing melt".to_string(),
                )),
            }),
            expiry_secs: 600,
            payment_hash: hex::encode(held_hash.as_byte_array()),
        })
        .await)?;
    let failure_quote = wallet
        .melt_quote(PaymentMethod::BOLT11, held_response.invoice, None, None)
        .await
        .context("failing cashu melt quote")?;
    let before_failure = wallet.total_balance().await?;
    let claimable_stream = rpc(payer.client.subscribe_events().await)?;
    let prepared = wallet
        .prepare_melt(&failure_quote.id, Default::default())
        .await
        .context("prepare failing cashu melt")?;
    let outcome = prepared
        .confirm_prefer_async()
        .await
        .context("dispatch failing cashu melt")?;
    match outcome {
        MeltOutcome::Pending(_) => {}
        MeltOutcome::Paid(_) => bail!("failing melt settled before the HTLC could be failed"),
    }
    await_claimable(claimable_stream, *held_hash.as_byte_array()).await?;
    rpc(payer
        .client
        .bolt11_fail_for_hash(Bolt11FailForHashRequest {
            payment_hash: hex::encode(held_hash.as_byte_array()),
        })
        .await)
    .context("fail held HTLC behind Cashu melt")?;

    // The mint may flip the quote to Unpaid right after a recovery pass,
    // so keep recovering until the reserved proofs are actually refunded.
    let reset_quote = eventually("failed Cashu melt compensated", NETWORK_TIMEOUT, || async {
        let _ = wallet.finalize_pending_melts().await?;
        let quote = wallet.check_melt_quote_status(&failure_quote.id).await?;
        if quote.state != CashuMeltState::Unpaid {
            return Ok(None);
        }
        let balance = wallet.total_balance().await?;
        Ok((balance == before_failure).then_some(quote))
    })
    .await?;
    assert!(reset_quote.payment_proof.is_none());
    assert_eq!(wallet.total_balance().await?, before_failure);

    mint.stop().await?;
    process.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires git, bitcoind, and builds the pinned ldk-server daemon; run `just test-regtest`"]
async fn ldk_server_regtest_suite() -> Result<()> {
    let root = std::env::var("TEST_DIRECTORY")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/ldk-server-regtest")
        });
    let run_dir = root.join(format!("run-{}", unix_now()));
    std::fs::create_dir_all(run_dir.join("logs"))?;

    let binary = ensure_ldk_server_binary()?;
    let bitcoind = Bitcoind::start().context("start bitcoind")?;
    eprintln!("bitcoind ready at height {}", bitcoind.block_count()?);

    let payer = LdkNode::start(
        &bitcoind,
        "ldk-regtest-payer",
        run_dir.join("node-payer"),
        &binary,
    )
    .await
    .context("start payer node")?;
    eprintln!("payer node ready: {}", payer.node_id);
    let mint_node = LdkNode::start(
        &bitcoind,
        "ldk-regtest-mint",
        run_dir.join("node-mint"),
        &binary,
    )
    .await
    .context("start mint-side node")?;
    eprintln!("mint node ready: {}", mint_node.node_id);

    open_channel(&bitcoind, &payer, &mint_node)
        .await
        .context("open funded channel")?;
    eprintln!("channel ready with {CHANNEL_SATS} sats");

    let backend = LdkServerBackend::new(backend_config(&mint_node))?;

    settings_scenario(&backend)
        .await
        .context("settings scenario")?;
    bolt11_receive_scenario(&payer, &backend)
        .await
        .context("bolt11 receive")?;
    bolt12_receive_scenario(&payer, &backend)
        .await
        .context("bolt12 receive")?;
    bolt11_send_scenario(&payer, &backend)
        .await
        .context("bolt11 send")?;
    amountless_send_scenario(&payer, &backend)
        .await
        .context("amountless send")?;
    pending_then_failed_send_scenario(&payer, &backend)
        .await
        .context("pending then failed send")?;
    expiry_scenarios(&payer, &backend)
        .await
        .context("expiry scenarios")?;

    let logs = run_dir.join("logs");
    processor_scenario(&payer, &mint_node, &logs)
        .await
        .context("black-box processor process")?;
    let process = ProcessorProcess::spawn(&mint_node, pick_port(), &logs).await?;
    cashu_scenario(&payer, &run_dir, process)
        .await
        .context("full Cashu mint/melt")?;

    eprintln!("regtest artifacts kept in {}", run_dir.display());
    Ok(())
}
