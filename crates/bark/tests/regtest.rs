use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use ark::lightning::Preimage;
use ark_testing::context::LightningPaymentSetup;
use ark_testing::{btc, sat, Bitcoind, TestContext};
use bitcoin::hashes::{sha256, Hash};
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use bitcoincore_rpc::RpcApi;
use cdk::amount::SplitTarget;
use cdk::mint::{MintBuilder, MintMeltLimits};
use cdk::nuts::{CurrencyUnit as CashuUnit, MeltQuoteState as CashuMeltState};
use cdk::nuts::{MintQuoteState, PaymentMethod, ProofsMethods};
use cdk::types::QuoteTTL;
use cdk::wallet::{MeltOutcome, Wallet};
use cdk_common::amount::Amount;
use cdk_common::payment::{
    Bolt11IncomingPaymentOptions, Bolt11OutgoingPaymentOptions, CreateIncomingPaymentResponse,
    CustomOutgoingPaymentOptions, Event, IncomingPaymentOptions, MakePaymentResponse, MintPayment,
    OnchainIncomingPaymentOptions, OnchainOutgoingPaymentOptions, OutgoingPaymentOptions,
    PaymentIdentifier, PaymentQuoteResponse, SettingsResponse, WaitPaymentResponse,
};
use cdk_common::{CurrencyUnit, MeltQuoteResponse, MeltQuoteState, QuoteId};
use cdk_payment_processor::PaymentProcessorClient;
use cdk_payment_processor_bark::backend::BarkBackend;
use cdk_payment_processor_bark::settings::BackendConfig;
use cln_rpc::plugins::hold;
use futures::{Stream, StreamExt};
use lightning_invoice::{Bolt11Invoice, Currency, InvoiceBuilder, PaymentSecret};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const MNEMONIC_A: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
const MNEMONIC_B: &str =
    "legal winner thank year wave sausage worth useful legal winner thank yellow";
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const NETWORK_TIMEOUT: Duration = Duration::from_secs(90);
const FEE_RESERVE_PADDING_SAT: u64 = 50;
static NEXT_PROCESS_LOG_ID: AtomicU64 = AtomicU64::new(0);

async fn fresh_test_context(name: &str) -> TestContext {
    let mut ctx = TestContext::new_minimal(name).await;
    let mut config = ctx.bitcoind_default_cfg("bitcoind");
    config.wallet = true;
    config.snapshot_dir = None;

    let bitcoind = Bitcoind::new("bitcoind".to_string(), config, None);
    bitcoind.start().await.expect("start bitcoind");
    bitcoind.create_wallet("central").await;
    bitcoind.prepare_funds().await;
    ctx.bitcoind = Some(Arc::new(bitcoind));

    ctx.init_central_electrs().await;
    ctx.init_central_postgres().await;
    ctx
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_secs()
}

fn backend_config(
    datadir: impl AsRef<Path>,
    mnemonic: &str,
    ark_url: &str,
    esplora_url: &str,
) -> BackendConfig {
    BackendConfig {
        mnemonic: mnemonic.to_string(),
        server_address: ark_url.to_string(),
        esplora_address: esplora_url.to_string(),
        network: "regtest".to_string(),
        data_dir: datadir.as_ref().to_string_lossy().into_owned(),
        event_poll_interval_ms: POLL_INTERVAL.as_millis() as u64,
    }
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

async fn cashu_melt_quote_state(wallet: &Wallet, quote_id: &str) -> Result<CashuMeltState> {
    let response = wallet
        .mint_connector()
        .get_melt_quote_status(PaymentMethod::BOLT11, quote_id)
        .await?;
    match response {
        MeltQuoteResponse::Bolt11(response) => Ok(response.state),
        response => bail!(
            "expected a BOLT11 melt quote response, got {}",
            response.method()
        ),
    }
}

fn bolt11_options(
    invoice: Bolt11Invoice,
    quote_id: QuoteId,
    max_fee_sat: u64,
) -> OutgoingPaymentOptions {
    OutgoingPaymentOptions::Bolt11(Box::new(Bolt11OutgoingPaymentOptions {
        bolt11: invoice,
        max_fee_amount: Some(Amount::new(max_fee_sat, CurrencyUnit::Sat)),
        timeout_secs: Some(30),
        melt_options: None,
        quote_id,
    }))
}

fn expired_invoice(amount_sat: u64) -> Bolt11Invoice {
    let key = SecretKey::from_slice(&[42; 32]).expect("valid secret key");
    InvoiceBuilder::new(Currency::Regtest)
        .description("expired regtest invoice".into())
        .payment_hash(sha256::Hash::from_slice(&[7; 32]).expect("32-byte hash"))
        .payment_secret(PaymentSecret([8; 32]))
        .amount_milli_satoshis(amount_sat * 1_000)
        .duration_since_epoch(Duration::from_secs(1))
        .expiry_time(Duration::from_secs(1))
        .min_final_cltv_expiry_delta(18)
        .build_signed(|hash| Secp256k1::new().sign_ecdsa_recoverable(hash, &key))
        .expect("build expired invoice")
}

fn unreachable_invoice(amount_sat: u64, marker: u8) -> Bolt11Invoice {
    let key = SecretKey::from_slice(&[marker; 32]).expect("valid secret key");
    InvoiceBuilder::new(Currency::Regtest)
        .description("unreachable regtest invoice".into())
        .payment_hash(
            sha256::Hash::from_slice(&[marker.saturating_add(1); 32]).expect("32-byte hash"),
        )
        .payment_secret(PaymentSecret([marker.saturating_add(2); 32]))
        .amount_milli_satoshis(amount_sat * 1_000)
        .duration_since_epoch(Duration::from_secs(unix_now()))
        .expiry_time(Duration::from_secs(120))
        .min_final_cltv_expiry_delta(18)
        .build_signed(|hash| Secp256k1::new().sign_ecdsa_recoverable(hash, &key))
        .expect("build unreachable invoice")
}

async fn hold_invoice(
    lightning: &LightningPaymentSetup,
    amount_sat: u64,
    marker: u8,
    description: &str,
) -> Result<(Bolt11Invoice, Preimage)> {
    let preimage = Preimage::from([marker; 32]);
    let payment_hash = preimage.compute_payment_hash();
    let mut hold_client = lightning.external.hold_client().await;
    let invoice = hold_client
        .invoice(hold::InvoiceRequest {
            payment_hash: payment_hash.as_ref().to_vec(),
            amount_msat: amount_sat * 1_000,
            description: Some(hold::invoice_request::Description::Memo(
                description.to_string(),
            )),
            min_final_cltv_expiry: Some(18),
            expiry: Some(3_600),
            routing_hints: vec![],
        })
        .await
        .context("create CLN hold invoice")?
        .into_inner()
        .bolt11;
    Ok((Bolt11Invoice::from_str(&invoice)?, preimage))
}

async fn settle_hold_invoice(lightning: &LightningPaymentSetup, preimage: Preimage) -> Result<()> {
    wait_for_hold_invoice_accepted(lightning, preimage).await?;
    lightning
        .external
        .hold_client()
        .await
        .settle(hold::SettleRequest {
            payment_preimage: preimage.as_ref().to_vec(),
        })
        .await
        .context("settle CLN hold invoice")?;
    Ok(())
}

async fn wait_for_hold_invoice_accepted(
    lightning: &LightningPaymentSetup,
    preimage: Preimage,
) -> Result<()> {
    let payment_hash = preimage.compute_payment_hash();
    eventually("held HTLC acceptance", NETWORK_TIMEOUT, || async {
        let mut hold_client = lightning.external.hold_client().await;
        let response = hold_client
            .list(hold::ListRequest {
                constraint: Some(hold::list_request::Constraint::PaymentHash(
                    payment_hash.as_ref().to_vec(),
                )),
            })
            .await?
            .into_inner();
        Ok(response
            .invoices
            .iter()
            .any(|invoice| invoice.state == hold::InvoiceState::Accepted as i32)
            .then_some(()))
    })
    .await
}

async fn next_event(
    backend: &BarkBackend,
    description: &str,
) -> Result<(
    Event,
    std::pin::Pin<Box<dyn futures::Stream<Item = Event> + Send>>,
)> {
    let mut stream = backend.wait_payment_event().await?;
    let event = tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
        .await
        .with_context(|| format!("timed out waiting for {description}"))?
        .with_context(|| format!("event stream ended while waiting for {description}"))?;
    Ok((event, stream))
}

async fn wait_for_incoming(
    backend: &BarkBackend,
    identifier: &PaymentIdentifier,
) -> Result<cdk_common::payment::WaitPaymentResponse> {
    eventually("incoming payment settlement", NETWORK_TIMEOUT, || async {
        let payments = backend
            .check_incoming_payment_status(identifier)
            .await
            .map_err(anyhow::Error::from)?;
        Ok(payments.into_iter().next())
    })
    .await
}

async fn wait_for_outgoing(
    backend: &BarkBackend,
    quote_id: &QuoteId,
) -> Result<cdk_common::payment::MakePaymentResponse> {
    eventually("outgoing payment settlement", NETWORK_TIMEOUT, || async {
        let response = backend
            .check_outgoing_payment(&PaymentIdentifier::QuoteId(quote_id.clone()))
            .await
            .map_err(anyhow::Error::from)?;
        Ok((response.status == MeltQuoteState::Paid).then_some(response))
    })
    .await
}

struct ProcessorProcess {
    child: Child,
    port: u16,
}

impl ProcessorProcess {
    fn spawn_child(config: &BackendConfig, port: u16, log_dir: &Path) -> Result<Child> {
        std::fs::create_dir_all(log_dir)?;
        let log_id = NEXT_PROCESS_LOG_ID.fetch_add(1, Ordering::Relaxed);
        let stdout =
            std::fs::File::create(log_dir.join(format!("processor-{port}-{log_id}.stdout.log")))?;
        let stderr =
            std::fs::File::create(log_dir.join(format!("processor-{port}-{log_id}.stderr.log")))?;
        Command::new(env!("CARGO_BIN_EXE_cdk-payment-processor-bark"))
            .current_dir(log_dir)
            .env("BARK_MNEMONIC", &config.mnemonic)
            .env("BARK_SERVER_ADDRESS", &config.server_address)
            .env("BARK_ESPLORA_ADDRESS", &config.esplora_address)
            .env("BARK_NETWORK", &config.network)
            .env("BARK_DATA_DIR", &config.data_dir)
            .env(
                "BARK_EVENT_POLL_INTERVAL_MS",
                config.event_poll_interval_ms.to_string(),
            )
            .env("SERVER_ADDRESS", "127.0.0.1")
            .env("SERVER_PORT", port.to_string())
            .env("ALLOW_INSECURE", "true")
            .env("RUST_LOG", "debug")
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .context("spawn Bark payment processor")
    }

    async fn spawn(config: &BackendConfig, port: u16, log_dir: &Path) -> Result<Self> {
        let child = Self::spawn_child(config, port, log_dir)?;

        let process = Self { child, port };
        process.client().await?;
        Ok(process)
    }

    async fn assert_startup_rejected(
        config: &BackendConfig,
        port: u16,
        log_dir: &Path,
    ) -> Result<()> {
        let mut child = Self::spawn_child(config, port, log_dir)?;
        match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
            Ok(status) => {
                let status = status?;
                if status.success() {
                    bail!("second processor unexpectedly opened the active data directory");
                }
            }
            Err(_) => {
                child.kill().await?;
                let _ = child.wait().await?;
                bail!("second processor did not reject the active data directory");
            }
        }
        Ok(())
    }

    async fn client(&self) -> Result<PaymentProcessorClient> {
        eventually(
            "payment processor readiness",
            Duration::from_secs(30),
            || async {
                match PaymentProcessorClient::new("127.0.0.1", self.port, None).await {
                    Ok(client) => Ok(Some(client)),
                    Err(error) => Ok::<_, anyhow::Error>({
                        tracing::debug!("payment processor not ready: {error}");
                        None
                    }),
                }
            },
        )
        .await
    }

    async fn stop(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.child.kill().await?;
        }
        let _ = self.child.wait().await?;
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
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
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
            .with_name("Bark regtest mint".to_string())
            .with_description("Bark payment processor regtest".to_string())
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
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
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

async fn wallet_lifecycle(
    config: &BackendConfig,
    other_datadir: &Path,
    empty_send_address: &str,
    insufficient_invoice: Bolt11Invoice,
    logs: &Path,
) -> Result<BarkBackend> {
    let backend = BarkBackend::new(config).await.context("fresh wallet")?;
    assert_eq!(backend.regtest_spendable_balance_sat().await?, 0);
    let identity = backend.regtest_new_ark_address().await?;

    let second_open = tokio::time::timeout(Duration::from_secs(5), BarkBackend::new(config)).await;
    assert!(
        !matches!(second_open, Ok(Ok(_))),
        "a second backend unexpectedly opened the active data directory"
    );
    ProcessorProcess::assert_startup_rejected(config, ark_testing::ports::pick_port(), logs)
        .await?;

    let other = BarkBackend::new(&BackendConfig {
        mnemonic: MNEMONIC_B.to_string(),
        data_dir: other_datadir.to_string_lossy().into_owned(),
        ..config.clone()
    })
    .await?;
    assert_ne!(identity, other.regtest_new_ark_address().await?);

    let insufficient_quote_id = QuoteId::new();
    let quote = other
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(
                insufficient_invoice.clone(),
                insufficient_quote_id.clone(),
                u64::MAX,
            ),
        )
        .await?;
    assert!(other
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(
                insufficient_invoice,
                insufficient_quote_id,
                quote.fee.to_u64(),
            ),
        )
        .await
        .is_err());
    assert_eq!(other.regtest_spendable_balance_sat().await?, 0);
    drop(other);

    for mnemonic in ["", "not a mnemonic"] {
        let error = BarkBackend::new(&BackendConfig {
            mnemonic: mnemonic.to_string(),
            data_dir: other_datadir.join("invalid").to_string_lossy().into_owned(),
            ..config.clone()
        })
        .await
        .err()
        .expect("invalid mnemonic must fail");
        assert!(error.to_string().contains("Invalid mnemonic"));
    }

    let endpoint_mismatch = BarkBackend::new(&BackendConfig {
        network: "signet".to_string(),
        data_dir: other_datadir
            .join("wrong-network")
            .to_string_lossy()
            .into_owned(),
        ..config.clone()
    })
    .await;
    assert!(endpoint_mismatch.is_err());

    let empty_send = OutgoingPaymentOptions::Onchain(Box::new(OnchainOutgoingPaymentOptions {
        address: empty_send_address.to_string(),
        amount: Amount::new(50_000, CurrencyUnit::Sat),
        max_fee_amount: Some(Amount::new(100_000, CurrencyUnit::Sat)),
        quote_id: QuoteId::new(),
        fee_index: None,
        metadata: None,
    }));
    assert!(backend
        .make_payment(&CurrencyUnit::Sat, empty_send)
        .await
        .is_err());
    assert_eq!(backend.regtest_spendable_balance_sat().await?, 0);

    drop(backend);
    let reopened = BarkBackend::new(config).await.context("restart wallet")?;
    assert_eq!(identity, reopened.regtest_peek_ark_address(0).await?);
    Ok(reopened)
}

async fn onchain_scenarios(
    ctx: &TestContext,
    config: &BackendConfig,
    mut backend: BarkBackend,
) -> Result<BarkBackend> {
    let quote_id = QuoteId::new();
    let request = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Onchain(
            OnchainIncomingPaymentOptions {
                quote_id: quote_id.clone(),
            },
        ))
        .await?;
    let gross_sat = 1_000_000;
    let deposit_txid = ctx
        .bitcoind()
        .fund_addr(&request.request, sat(gross_sat))
        .await;
    let identifier = PaymentIdentifier::QuoteId(quote_id.clone());
    assert!(backend
        .check_incoming_payment_status(&identifier)
        .await?
        .is_empty());

    ctx.generate_blocks(1).await;
    assert!(backend
        .check_incoming_payment_status(&identifier)
        .await?
        .is_empty());

    // The first confirmed status check has persisted and broadcast the board.
    // Restart at that boundary, then let Bark recover and finalize it.
    drop(backend);
    backend = BarkBackend::new(config).await?;
    ctx.generate_blocks(ark_testing::constants::BOARD_CONFIRMATIONS)
        .await;
    let payment = wait_for_incoming(&backend, &identifier).await?;
    assert_eq!(payment.payment_identifier, identifier);
    let net_sat = payment.payment_amount.to_u64();
    assert!(net_sat > 0);
    assert!(net_sat < gross_sat);
    assert_ne!(payment.payment_id, deposit_txid.to_string());

    let recipient = ctx.bitcoind().get_new_address();
    let send_quote_id = QuoteId::new();
    let send_options = OutgoingPaymentOptions::Onchain(Box::new(OnchainOutgoingPaymentOptions {
        address: recipient.to_string(),
        amount: Amount::new(40_000, CurrencyUnit::Sat),
        max_fee_amount: Some(Amount::new(100_000, CurrencyUnit::Sat)),
        quote_id: send_quote_id.clone(),
        fee_index: None,
        metadata: None,
    }));
    let quote = backend
        .get_payment_quote(&CurrencyUnit::Sat, send_options.clone())
        .await?;
    assert_eq!(
        quote.request_lookup_id,
        Some(PaymentIdentifier::QuoteId(send_quote_id.clone()))
    );
    assert_eq!(quote.fee_options.as_ref().map(Vec::len), Some(1));
    assert_eq!(quote.fee_options.as_ref().unwrap()[0].fee_index, 0);

    let pending = backend
        .make_payment(&CurrencyUnit::Sat, send_options.clone())
        .await?;
    assert_eq!(pending.status, MeltQuoteState::Pending);
    let duplicate = backend
        .make_payment(&CurrencyUnit::Sat, send_options.clone())
        .await?;
    assert_eq!(pending.payment_proof, duplicate.payment_proof);

    drop(backend);
    backend = BarkBackend::new(config).await?;
    ctx.generate_blocks(1).await;
    let paid = wait_for_outgoing(&backend, &send_quote_id).await?;
    assert_eq!(paid.payment_proof, pending.payment_proof);
    assert_eq!(
        ctx.bitcoind().get_received_by_address(&recipient).to_sat(),
        40_000
    );
    assert_eq!(paid.total_spent.to_u64(), 40_000 + quote.fee.to_u64());

    let invalid_fee = OutgoingPaymentOptions::Onchain(Box::new(OnchainOutgoingPaymentOptions {
        fee_index: Some(99),
        quote_id: QuoteId::new(),
        ..match send_options.clone() {
            OutgoingPaymentOptions::Onchain(options) => *options,
            _ => unreachable!(),
        }
    }));
    assert!(backend
        .make_payment(&CurrencyUnit::Sat, invalid_fee)
        .await
        .is_err());

    let mainnet_address =
        bitcoin::Address::from_script(&recipient.script_pubkey(), bitcoin::Network::Bitcoin)?;
    let wrong_network = OutgoingPaymentOptions::Onchain(Box::new(OnchainOutgoingPaymentOptions {
        address: mainnet_address.to_string(),
        quote_id: QuoteId::new(),
        ..match send_options {
            OutgoingPaymentOptions::Onchain(options) => *options,
            _ => unreachable!(),
        }
    }));
    assert!(backend
        .get_payment_quote(&CurrencyUnit::Sat, wrong_network)
        .await
        .is_err());

    Ok(backend)
}

async fn lightning_scenarios(
    lightning: &LightningPaymentSetup,
    backend: &BarkBackend,
) -> Result<()> {
    let amount_sat = 120_000;
    let response = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("Bark regtest receive".to_string()),
                amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 120),
            },
        ))
        .await?;
    let invoice = Bolt11Invoice::from_str(&response.request)?;
    let invoice_payment_hash: [u8; 32] = *invoice.payment_hash().as_ref();
    assert_eq!(invoice.amount_milli_satoshis(), Some(amount_sat * 1_000));
    assert_eq!(invoice.description().to_string(), "Bark regtest receive");
    assert_eq!(
        response.expiry,
        invoice.expires_at().map(|duration| duration.as_secs())
    );
    assert!(response.expiry.is_some_and(|expiry| expiry > unix_now()));
    assert!(backend
        .check_incoming_payment_status(&response.request_lookup_id)
        .await?
        .is_empty());

    let mut stream = backend.wait_payment_event().await?;
    let (pay_result, event_result) = tokio::join!(
        lightning.external.try_pay_bolt11(&response.request),
        tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
    );
    pay_result?;
    let event = event_result?.context("receive event stream ended")?;
    let Event::PaymentReceived(received) = event else {
        bail!("unexpected event while waiting for lightning receive")
    };
    assert_eq!(received.payment_identifier, response.request_lookup_id);
    assert_eq!(received.payment_amount.to_u64(), amount_sat);
    assert_eq!(received.payment_id, hex::encode(invoice_payment_hash));
    assert_eq!(
        backend
            .check_incoming_payment_status(&response.request_lookup_id)
            .await?
            .len(),
        1
    );

    drop(stream);
    let mut reconnect = backend.wait_payment_event().await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), reconnect.next())
            .await
            .is_err()
    );
    backend.cancel_payment_event_stream();
    drop(reconnect);

    // The topology uses a short server-side expiry so a real expired Bark
    // invoice can be exercised without waiting minutes. CLN must reject it,
    // and Bark must never turn it into mint credit.
    let expiring = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("Bark expired receive".to_string()),
                amount: Amount::new(20_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 120),
            },
        ))
        .await?;
    let expiring_invoice = Bolt11Invoice::from_str(&expiring.request)?;
    let balance_before_expired_receive = backend.regtest_spendable_balance_sat().await?;
    eventually(
        "Bark receive invoice expiry",
        Duration::from_secs(15),
        || async { Ok(expiring_invoice.is_expired().then_some(())) },
    )
    .await?;
    assert!(lightning
        .external
        .try_pay_bolt11(&expiring.request)
        .await
        .is_err());
    assert!(backend
        .check_incoming_payment_status(&expiring.request_lookup_id)
        .await?
        .is_empty());
    assert_eq!(
        backend.regtest_spendable_balance_sat().await?,
        balance_before_expired_receive
    );

    // Multiple simultaneous receives must retain their quote/amount
    // correlation even though the event stream emits them one at a time.
    let mut concurrent_receives = Vec::new();
    for amount_sat in [31_001, 31_002, 31_003] {
        let response = backend
            .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
                Bolt11IncomingPaymentOptions {
                    description: Some(format!("concurrent receive {amount_sat}")),
                    amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                    unix_expiry: Some(unix_now() + 120),
                },
            ))
            .await?;
        concurrent_receives.push((amount_sat, response));
    }
    let mut concurrent_stream = backend.wait_payment_event().await?;
    let pay_all = async {
        futures::future::try_join_all(
            concurrent_receives
                .iter()
                .map(|(_, response)| lightning.external.try_pay_bolt11(&response.request)),
        )
        .await?;
        Ok::<(), anyhow::Error>(())
    };
    let collect_all = async {
        let mut received = std::collections::HashMap::new();
        while received.len() < concurrent_receives.len() {
            if let Some(Event::PaymentReceived(event)) = concurrent_stream.next().await {
                received.insert(event.payment_identifier, event.payment_amount.to_u64());
            }
        }
        Ok::<_, anyhow::Error>(received)
    };
    let (pay_result, received) =
        tokio::join!(pay_all, tokio::time::timeout(NETWORK_TIMEOUT, collect_all));
    pay_result?;
    let received = received.context("concurrent receive events timed out")??;
    for (amount_sat, response) in &concurrent_receives {
        assert_eq!(received.get(&response.request_lookup_id), Some(amount_sat));
        assert_eq!(
            wait_for_incoming(backend, &response.request_lookup_id)
                .await?
                .payment_amount
                .to_u64(),
            *amount_sat
        );
    }
    backend.cancel_payment_event_stream();
    drop(concurrent_stream);

    // A paid send must report the real preimage and actual total spent, emit
    // one terminal event, and remain idempotent after a simulated lost reply.
    let preimage = [11; 32];
    let outgoing = lightning
        .external
        .invoice_with_preimage(
            Some(sat(25_000)),
            format!("bark-send-{}", QuoteId::new()),
            "Bark outgoing",
            preimage,
        )
        .await;
    let outgoing_invoice = Bolt11Invoice::from_str(&outgoing)?;
    let quote_id = QuoteId::new();
    let quote_options = bolt11_options(outgoing_invoice.clone(), quote_id.clone(), u64::MAX);
    let quote = backend
        .get_payment_quote(&CurrencyUnit::Sat, quote_options.clone())
        .await?;
    assert_eq!(quote.amount.to_u64(), 25_000);
    let quoted_fee_sat = quote.fee.to_u64();
    assert!(quoted_fee_sat > 0, "test server should charge a fee");

    let balance_before_low_cap = backend.regtest_spendable_balance_sat().await?;
    let low_cap = bolt11_options(outgoing_invoice.clone(), QuoteId::new(), quoted_fee_sat - 1);
    assert!(backend
        .make_payment(&CurrencyUnit::Sat, low_cap)
        .await
        .is_err());
    assert_eq!(
        backend.regtest_spendable_balance_sat().await?,
        balance_before_low_cap
    );

    let options = bolt11_options(outgoing_invoice, quote_id.clone(), quoted_fee_sat);
    let first = backend
        .make_payment(&CurrencyUnit::Sat, options.clone())
        .await?;
    assert!(matches!(
        first.status,
        MeltQuoteState::Pending | MeltQuoteState::Paid
    ));

    let (event, event_stream) = next_event(backend, "lightning send terminal event").await?;
    let Event::PaymentSuccessful {
        quote_id: event_quote,
        details,
    } = event
    else {
        bail!("unexpected terminal event for lightning send")
    };
    assert_eq!(event_quote, quote_id);
    assert_eq!(details.status, MeltQuoteState::Paid);
    assert_eq!(details.payment_proof, Some(hex::encode(preimage)));
    assert_eq!(
        details.total_spent.clone().to_u64(),
        25_000 + quoted_fee_sat
    );
    backend.cancel_payment_event_stream();
    drop(event_stream);

    let retry = backend
        .make_payment(&CurrencyUnit::Sat, options.clone())
        .await?;
    assert_eq!(retry, details);
    assert_eq!(
        backend
            .check_outgoing_payment(&PaymentIdentifier::QuoteId(quote_id.clone()))
            .await?,
        details
    );
    let (same_a, same_b) = tokio::join!(
        backend.make_payment(&CurrencyUnit::Sat, options.clone()),
        backend.make_payment(&CurrencyUnit::Sat, options)
    );
    assert_eq!(same_a?, same_b?);

    // Submit the same quote concurrently on its first attempt while a hold
    // invoice keeps the HTLC pending. Both callers must see the same intent,
    // Bark must record one external movement, and settlement must be visible
    // through the event stream and a subsequent status poll.
    let (held_invoice, held_preimage) =
        hold_invoice(lightning, 14_000, 14, "concurrent duplicate send").await?;
    let held_hash: [u8; 32] = *held_invoice.payment_hash().as_ref();
    let held_quote_id = QuoteId::new();
    let held_quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(held_invoice.clone(), held_quote_id.clone(), u64::MAX),
        )
        .await?;
    let held_options = bolt11_options(held_invoice, held_quote_id.clone(), held_quote.fee.to_u64());
    let mut held_stream = backend.wait_payment_event().await?;
    let (held_a, held_b) = tokio::join!(
        backend.make_payment(&CurrencyUnit::Sat, held_options.clone()),
        backend.make_payment(&CurrencyUnit::Sat, held_options)
    );
    let held_a = held_a?;
    let held_b = held_b?;
    assert_eq!(held_a, held_b);
    assert_eq!(held_a.status, MeltQuoteState::Pending);
    assert_eq!(
        backend.regtest_lightning_movement_count(held_hash).await?,
        1
    );
    settle_hold_invoice(lightning, held_preimage).await?;
    let held_event = tokio::time::timeout(NETWORK_TIMEOUT, held_stream.next())
        .await
        .context("held duplicate payment did not emit a terminal event")?
        .context("event stream ended before held duplicate payment settled")?;
    let Event::PaymentSuccessful {
        quote_id: event_quote_id,
        details: held_paid,
    } = held_event
    else {
        bail!("unexpected event for held duplicate payment")
    };
    assert_eq!(event_quote_id, held_quote_id);
    assert_eq!(held_paid.status, MeltQuoteState::Paid);
    assert_eq!(held_paid.payment_proof, Some(held_preimage.to_string()));
    assert_eq!(
        backend
            .check_outgoing_payment(&PaymentIdentifier::QuoteId(held_quote_id))
            .await?,
        held_paid
    );
    assert_eq!(
        backend.regtest_lightning_movement_count(held_hash).await?,
        1
    );
    backend.cancel_payment_event_stream();
    drop(held_stream);

    let expired = expired_invoice(1_000);
    let balance_before_expired_send = backend.regtest_spendable_balance_sat().await?;
    assert!(backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(expired.clone(), QuoteId::new(), 1_000)
        )
        .await
        .is_err());
    assert_eq!(
        backend.regtest_spendable_balance_sat().await?,
        balance_before_expired_send
    );
    assert!(backend
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(expired, QuoteId::new(), 1_000)
        )
        .await
        .is_err());
    assert_eq!(
        backend.regtest_spendable_balance_sat().await?,
        balance_before_expired_send
    );

    // Different quotes may be submitted concurrently. Bark serializes wallet
    // mutation internally, but each result must remain attached to its own
    // invoice and preimage.
    let concurrent_a_preimage = [12; 32];
    let concurrent_b_preimage = [13; 32];
    let concurrent_a = Bolt11Invoice::from_str(
        &lightning
            .external
            .invoice_with_preimage(
                Some(sat(10_001)),
                format!("bark-concurrent-a-{}", QuoteId::new()),
                "concurrent A",
                concurrent_a_preimage,
            )
            .await,
    )?;
    let concurrent_b = Bolt11Invoice::from_str(
        &lightning
            .external
            .invoice_with_preimage(
                Some(sat(10_002)),
                format!("bark-concurrent-b-{}", QuoteId::new()),
                "concurrent B",
                concurrent_b_preimage,
            )
            .await,
    )?;
    let concurrent_a_quote = QuoteId::new();
    let concurrent_b_quote = QuoteId::new();
    let quote_a_options =
        bolt11_options(concurrent_a.clone(), concurrent_a_quote.clone(), u64::MAX);
    let quote_b_options =
        bolt11_options(concurrent_b.clone(), concurrent_b_quote.clone(), u64::MAX);
    let (quote_a, quote_b) = tokio::join!(
        backend.get_payment_quote(&CurrencyUnit::Sat, quote_a_options),
        backend.get_payment_quote(&CurrencyUnit::Sat, quote_b_options)
    );
    let send_a = bolt11_options(
        concurrent_a,
        concurrent_a_quote.clone(),
        quote_a?.fee.to_u64(),
    );
    let send_b = bolt11_options(
        concurrent_b,
        concurrent_b_quote.clone(),
        quote_b?.fee.to_u64(),
    );
    let (started_a, started_b) = tokio::join!(
        backend.make_payment(&CurrencyUnit::Sat, send_a),
        backend.make_payment(&CurrencyUnit::Sat, send_b)
    );
    started_a?;
    started_b?;
    let (paid_a, paid_b) = tokio::join!(
        wait_for_outgoing(backend, &concurrent_a_quote),
        wait_for_outgoing(backend, &concurrent_b_quote)
    );
    assert_eq!(
        paid_a?.payment_proof,
        Some(hex::encode(concurrent_a_preimage))
    );
    assert_eq!(
        paid_b?.payment_proof,
        Some(hex::encode(concurrent_b_preimage))
    );

    let unreachable = unreachable_invoice(5_000, 43);
    let unreachable_quote_id = QuoteId::new();
    let quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(unreachable.clone(), unreachable_quote_id.clone(), u64::MAX),
        )
        .await?;
    let balance_before_failure = backend.regtest_spendable_balance_sat().await?;
    let mut failed_stream = backend.wait_payment_event().await?;
    let started = backend
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(
                unreachable,
                unreachable_quote_id.clone(),
                quote.fee.to_u64(),
            ),
        )
        .await?;
    assert!(
        matches!(
            started.status,
            MeltQuoteState::Pending | MeltQuoteState::Unpaid
        ),
        "an unreachable payment must never be reported as paid"
    );
    assert_eq!(started.total_spent.to_u64(), 0);
    let failed_event = tokio::time::timeout(NETWORK_TIMEOUT, failed_stream.next())
        .await
        .context("unreachable payment did not become terminal")?
        .context("event stream ended before unreachable payment failed")?;
    let Event::PaymentFailed {
        quote_id: failed_quote,
        ..
    } = failed_event
    else {
        bail!("unexpected event for unreachable payment")
    };
    assert_eq!(failed_quote, unreachable_quote_id);
    let failed_status = backend
        .check_outgoing_payment(&PaymentIdentifier::QuoteId(unreachable_quote_id.clone()))
        .await?;
    assert_eq!(failed_status.status, MeltQuoteState::Unpaid);
    assert_eq!(failed_status.total_spent.to_u64(), 0);
    backend.cancel_payment_event_stream();
    drop(failed_stream);
    eventually(
        "failed payment balance restoration",
        NETWORK_TIMEOUT,
        || async {
            let balance = backend.regtest_spendable_balance_sat().await?;
            Ok((balance == balance_before_failure).then_some(()))
        },
    )
    .await?;

    Ok(())
}

async fn stopped_receive_scenario(
    lightning: &LightningPaymentSetup,
    config: &BackendConfig,
    backend: BarkBackend,
) -> Result<BarkBackend> {
    let response = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("offline receive".to_string()),
                amount: Amount::new(30_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 120),
            },
        ))
        .await?;
    drop(backend);

    let mut payment = Box::pin(lightning.external.try_pay_bolt11(&response.request));
    tokio::select! {
        result = &mut payment => bail!("payment unexpectedly finished while processor was stopped: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(750)) => {}
    }

    let backend = BarkBackend::new(config).await?;
    let mut stream = backend.wait_payment_event().await?;
    let (payment_result, event_result) = tokio::join!(
        &mut payment,
        tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
    );
    payment_result?;
    let event = event_result?.context("offline receive event stream ended")?;
    let Event::PaymentReceived(received) = event else {
        bail!("unexpected event for offline receive")
    };
    assert_eq!(received.payment_identifier, response.request_lookup_id);
    backend.cancel_payment_event_stream();
    drop(stream);
    Ok(backend)
}

async fn pending_send_restart_scenario(
    lightning: &LightningPaymentSetup,
    config: &BackendConfig,
    backend: BarkBackend,
) -> Result<BarkBackend> {
    let (invoice, preimage) = hold_invoice(lightning, 16_000, 15, "pending send restart").await?;
    let payment_hash: [u8; 32] = *invoice.payment_hash().as_ref();
    let quote_id = QuoteId::new();
    let quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), quote_id.clone(), u64::MAX),
        )
        .await?;
    let options = bolt11_options(invoice, quote_id.clone(), quote.fee.to_u64());
    let started = backend
        .make_payment(&CurrencyUnit::Sat, options.clone())
        .await?;
    assert_eq!(started.status, MeltQuoteState::Pending);
    assert_eq!(
        backend
            .regtest_lightning_movement_count(payment_hash)
            .await?,
        1
    );
    wait_for_hold_invoice_accepted(lightning, preimage).await?;

    drop(backend);
    let backend = BarkBackend::new(config).await?;
    let retry = backend.make_payment(&CurrencyUnit::Sat, options).await?;
    assert_eq!(retry.status, MeltQuoteState::Pending);
    assert_eq!(
        backend
            .regtest_lightning_movement_count(payment_hash)
            .await?,
        1
    );

    let mut stream = backend.wait_payment_event().await?;
    settle_hold_invoice(lightning, preimage).await?;
    let event = tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
        .await
        .context("restarted pending send did not emit a terminal event")?
        .context("event stream ended before restarted pending send settled")?;
    let Event::PaymentSuccessful {
        quote_id: event_quote_id,
        details,
    } = event
    else {
        bail!("unexpected event for restarted pending send")
    };
    assert_eq!(event_quote_id, quote_id);
    assert_eq!(details.status, MeltQuoteState::Paid);
    assert_eq!(details.payment_proof, Some(preimage.to_string()));
    assert_eq!(
        backend
            .check_outgoing_payment(&PaymentIdentifier::QuoteId(quote_id))
            .await?,
        details
    );
    assert_eq!(
        backend
            .regtest_lightning_movement_count(payment_hash)
            .await?,
        1
    );
    backend.cancel_payment_event_stream();
    drop(stream);
    Ok(backend)
}

async fn arkoor_scenario(
    ctx: &TestContext,
    ark_url: &str,
    config: &BackendConfig,
    backend: BarkBackend,
) -> Result<BarkBackend> {
    let ark_url = ark_url.to_string();
    let receiver = ctx
        .bark_sdk("processor-arkoor-receiver", &ark_url)
        .create()
        .await;
    let address = receiver.new_address().await?;
    let amount_sat = 15_000;
    let quote_id = QuoteId::new();
    let options = OutgoingPaymentOptions::Custom(Box::new(CustomOutgoingPaymentOptions {
        method: "arkoor".to_string(),
        request: address.to_string(),
        amount: Some(Amount::new(amount_sat, CurrencyUnit::Sat)),
        max_fee_amount: Some(Amount::new(0, CurrencyUnit::Sat)),
        timeout_secs: Some(30),
        melt_options: None,
        extra_json: Some(serde_json::json!({"amount_sat": amount_sat}).to_string()),
        quote_id: quote_id.clone(),
    }));
    let quote = backend
        .get_payment_quote(&CurrencyUnit::Sat, options.clone())
        .await?;
    assert_eq!(quote.amount.to_u64(), amount_sat);
    assert_eq!(quote.fee.to_u64(), 0);
    assert_eq!(
        quote.extra_json,
        Some(serde_json::json!({"routing": "arkoor"}))
    );

    let paid = backend
        .make_payment(&CurrencyUnit::Sat, options.clone())
        .await?;
    assert_eq!(paid.status, MeltQuoteState::Paid);
    assert_eq!(paid.total_spent.clone().to_u64(), amount_sat);
    assert!(paid.payment_proof.is_some());

    drop(backend);
    let backend = BarkBackend::new(config).await?;
    assert_eq!(
        backend.make_payment(&CurrencyUnit::Sat, options).await?,
        paid
    );

    let (event, event_stream) = next_event(&backend, "arkoor terminal event").await?;
    let Event::PaymentSuccessful {
        quote_id: event_quote,
        details,
    } = event
    else {
        bail!("unexpected arkoor terminal event")
    };
    assert_eq!(event_quote, quote_id);
    assert_eq!(details, paid);
    backend.cancel_payment_event_stream();
    drop(event_stream);

    eventually("arkoor receiver balance", NETWORK_TIMEOUT, || async {
        receiver.sync().await;
        let balance = receiver.balance().await?.spendable.to_sat();
        Ok((balance == amount_sat).then_some(()))
    })
    .await?;

    Ok(backend)
}

async fn grpc_restart_scenario(
    lightning: &LightningPaymentSetup,
    config: &BackendConfig,
    logs: &Path,
) -> Result<ProcessorProcess> {
    let port = ark_testing::ports::pick_port();
    let mut process = ProcessorProcess::spawn(config, port, logs).await?;
    let client = process.client().await?;
    let settings = client.get_settings().await?;
    assert_eq!(settings.unit, "sat");
    let bolt11 = settings.bolt11.context("BOLT11 settings missing")?;
    assert!(!bolt11.mpp);
    assert!(!bolt11.amountless);
    assert!(bolt11.invoice_description);
    assert!(settings.bolt12.is_none());
    assert!(settings.onchain.is_some());
    assert!(settings.custom.contains_key("arkoor"));

    let response = client
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("gRPC restart receive".to_string()),
                amount: Amount::new(20_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 120),
            },
        ))
        .await?;
    drop(client);
    process.stop().await?;

    let mut payment = Box::pin(lightning.external.try_pay_bolt11(&response.request));
    tokio::select! {
        result = &mut payment => bail!("gRPC payment unexpectedly finished while processor was stopped: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(750)) => {}
    }

    process = ProcessorProcess::spawn(config, port, logs).await?;
    let client = process.client().await?;
    let mut stream = client.wait_payment_event().await?;
    let (payment_result, event_result) = tokio::join!(
        &mut payment,
        tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
    );
    payment_result?;
    let Event::PaymentReceived(received) = event_result?.context("gRPC event stream ended")? else {
        bail!("unexpected gRPC payment event")
    };
    assert_eq!(received.payment_identifier, response.request_lookup_id);
    assert_eq!(received.payment_amount.to_u64(), 20_000);

    let preimage = [21; 32];
    let invoice = Bolt11Invoice::from_str(
        &lightning
            .external
            .invoice_with_preimage(
                Some(sat(12_345)),
                format!("grpc-send-{}", QuoteId::new()),
                "gRPC Bark send",
                preimage,
            )
            .await,
    )?;
    let quote_id = QuoteId::new();
    let quote_options = bolt11_options(invoice.clone(), quote_id.clone(), u64::MAX);
    let quote = client
        .get_payment_quote(&CurrencyUnit::Sat, quote_options)
        .await?;
    let send_options = bolt11_options(invoice, quote_id.clone(), quote.fee.to_u64());
    client
        .make_payment(&CurrencyUnit::Sat, send_options)
        .await?;
    // Exercise both directions through one long-lived gRPC event stream. This
    // also matches how the mint consumes processor events in production.
    let event = tokio::time::timeout(NETWORK_TIMEOUT, stream.next())
        .await
        .context("gRPC send event timed out")?
        .context("gRPC send event stream ended")?;
    let Event::PaymentSuccessful {
        quote_id: event_quote,
        details,
    } = event
    else {
        bail!("unexpected gRPC outgoing event")
    };
    assert_eq!(event_quote, quote_id);
    assert_eq!(details.payment_proof, Some(hex::encode(preimage)));
    assert_eq!(
        client
            .check_outgoing_payment(&PaymentIdentifier::QuoteId(quote_id))
            .await?,
        details
    );
    client.cancel_payment_event_stream();
    drop(stream);
    Ok(process)
}

async fn cashu_scenario(
    lightning: &LightningPaymentSetup,
    config: &BackendConfig,
    logs: &Path,
    mut process: ProcessorProcess,
) -> Result<ProcessorProcess> {
    let mint_port = ark_testing::ports::pick_port();
    let mint_url = format!("http://127.0.0.1:{mint_port}");
    let mint_db = logs.join("cashu-mint.sqlite");
    let wallet_db = Arc::new(cdk_sqlite::wallet::memory::empty().await?);
    let seed = bip39::Mnemonic::parse(MNEMONIC_B)?.to_seed_normalized("");
    let mint_seed = bip39::Mnemonic::parse(MNEMONIC_A)?.to_seed_normalized("mint");

    let mint =
        MintHttpServer::start(process.client().await?, &mint_db, &mint_seed, mint_port).await?;
    let wallet = Wallet::new(&mint_url, CashuUnit::Sat, wallet_db, seed, None)?;
    assert_eq!(wallet.total_balance().await?.to_u64(), 0);

    let mint_quote = wallet
        .mint_quote(
            PaymentMethod::BOLT11,
            Some(cdk::Amount::from(100_000)),
            Some("full Bark mint".to_string()),
            None,
        )
        .await?;
    assert_eq!(mint_quote.state, MintQuoteState::Unpaid);
    let (pay_result, paid_quote) = tokio::join!(
        lightning.external.try_pay_bolt11(&mint_quote.request),
        eventually("Cashu mint quote paid", NETWORK_TIMEOUT, || async {
            let quote = wallet.check_mint_quote_status(&mint_quote.id).await?;
            Ok((quote.state == MintQuoteState::Paid).then_some(quote))
        })
    );
    pay_result?;
    let paid_quote = paid_quote?;
    assert_eq!(
        paid_quote.amount.map(|amount| amount.to_u64()),
        Some(100_000)
    );

    // Restart the mint after payment but before issuance. Its SQLite state and
    // the Bark processor's quote mapping must be sufficient to resume.
    mint.stop().await?;
    let mint =
        MintHttpServer::start(process.client().await?, &mint_db, &mint_seed, mint_port).await?;
    let proofs = wallet
        .mint(&mint_quote.id, SplitTarget::default(), None)
        .await?;
    assert_eq!(proofs.total_amount()?.to_u64(), 100_000);
    assert_eq!(wallet.total_balance().await?.to_u64(), 100_000);
    assert_eq!(
        wallet.check_mint_quote_status(&mint_quote.id).await?.state,
        MintQuoteState::Issued
    );
    assert!(wallet
        .mint(&mint_quote.id, SplitTarget::default(), None)
        .await
        .is_err());

    let mint_quote_a = wallet
        .mint_quote(
            PaymentMethod::BOLT11,
            Some(cdk::Amount::from(17_000)),
            Some("out-of-order mint A".to_string()),
            None,
        )
        .await?;
    let mint_quote_b = wallet
        .mint_quote(
            PaymentMethod::BOLT11,
            Some(cdk::Amount::from(19_000)),
            Some("out-of-order mint B".to_string()),
            None,
        )
        .await?;
    let pay_extra = async {
        let (a, b) = tokio::join!(
            lightning.external.try_pay_bolt11(&mint_quote_a.request),
            lightning.external.try_pay_bolt11(&mint_quote_b.request)
        );
        a?;
        b?;
        Ok::<(), anyhow::Error>(())
    };
    let wait_extra = async {
        let (a, b) = tokio::join!(
            eventually("Cashu mint quote A paid", NETWORK_TIMEOUT, || async {
                let quote = wallet.check_mint_quote_status(&mint_quote_a.id).await?;
                Ok((quote.state == MintQuoteState::Paid).then_some(()))
            }),
            eventually("Cashu mint quote B paid", NETWORK_TIMEOUT, || async {
                let quote = wallet.check_mint_quote_status(&mint_quote_b.id).await?;
                Ok((quote.state == MintQuoteState::Paid).then_some(()))
            })
        );
        a?;
        b?;
        Ok::<(), anyhow::Error>(())
    };
    let (paid, statuses) = tokio::join!(pay_extra, wait_extra);
    paid?;
    statuses?;

    // Issue in the opposite order to quote creation to catch accidental
    // reliance on "most recent quote" state.
    wallet
        .mint(&mint_quote_b.id, SplitTarget::default(), None)
        .await?;
    wallet
        .mint(&mint_quote_a.id, SplitTarget::default(), None)
        .await?;
    assert_eq!(wallet.total_balance().await?.to_u64(), 136_000);

    let (invoice, preimage) = hold_invoice(lightning, 30_000, 23, "Cashu pending melt").await?;
    let melt_quote = wallet
        .melt_quote(PaymentMethod::BOLT11, invoice.to_string(), None, None)
        .await?;
    assert_eq!(melt_quote.state, CashuMeltState::Unpaid);

    // The quote is owned by the mint, while Bark's durable payment state is
    // created during execution. Restarting here verifies that the real gRPC
    // channel and processor can recover before the melt begins.
    process.stop().await?;
    process = ProcessorProcess::spawn(config, process.port, logs).await?;
    process.client().await?.get_settings().await?;

    let before = wallet.total_balance().await?.to_u64();
    let outcome = wallet
        .prepare_melt(&melt_quote.id, Default::default())
        .await?
        .confirm_prefer_async()
        .await?;
    let pending = match outcome {
        MeltOutcome::Pending(pending) => pending,
        MeltOutcome::Paid(_) => bail!("hold-invoice melt settled before explicit release"),
    };
    drop(pending);
    wait_for_hold_invoice_accepted(lightning, preimage).await?;
    assert_eq!(
        cashu_melt_quote_state(&wallet, &melt_quote.id).await?,
        CashuMeltState::Pending
    );

    // Restart both sides while the Cashu saga and Bark payment are pending.
    // The wallet keeps its saga locally, the mint keeps its quote in SQLite,
    // and the processor keeps the single external payment intent.
    mint.stop().await?;
    process.stop().await?;
    process = ProcessorProcess::spawn(config, process.port, logs).await?;
    let mint =
        MintHttpServer::start(process.client().await?, &mint_db, &mint_seed, mint_port).await?;
    assert_eq!(
        cashu_melt_quote_state(&wallet, &melt_quote.id).await?,
        CashuMeltState::Pending
    );
    settle_hold_invoice(lightning, preimage).await?;
    let finalized = eventually(
        "Cashu melt recovery after mint and processor restart",
        NETWORK_TIMEOUT,
        || async {
            let finalized = wallet.finalize_pending_melts().await?;
            Ok(finalized
                .into_iter()
                .find(|melt| melt.quote_id() == melt_quote.id))
        },
    )
    .await?;
    assert_eq!(finalized.state(), CashuMeltState::Paid);
    let expected_preimage = preimage.to_string();
    assert_eq!(finalized.payment_proof(), Some(expected_preimage.as_str()));
    let after = wallet.total_balance().await?.to_u64();
    assert_eq!(
        before - after,
        finalized.amount().to_u64() + finalized.fee_paid().to_u64()
    );
    assert!(finalized.fee_paid().to_u64() < melt_quote.fee_reserve.to_u64());

    let out_of_order_a_preimage = [24; 32];
    let out_of_order_b_preimage = [25; 32];
    let out_of_order_a_invoice = lightning
        .external
        .invoice_with_preimage(
            Some(sat(6_000)),
            format!("cashu-melt-a-{}", QuoteId::new()),
            "Cashu melt A",
            out_of_order_a_preimage,
        )
        .await;
    let out_of_order_b_invoice = lightning
        .external
        .invoice_with_preimage(
            Some(sat(7_000)),
            format!("cashu-melt-b-{}", QuoteId::new()),
            "Cashu melt B",
            out_of_order_b_preimage,
        )
        .await;
    let out_of_order_a = wallet
        .melt_quote(PaymentMethod::BOLT11, out_of_order_a_invoice, None, None)
        .await?;
    let out_of_order_b = wallet
        .melt_quote(PaymentMethod::BOLT11, out_of_order_b_invoice, None, None)
        .await?;
    let prepared_a = wallet
        .prepare_melt(&out_of_order_a.id, Default::default())
        .await?;
    let prepared_b = wallet
        .prepare_melt(&out_of_order_b.id, Default::default())
        .await?;
    let finalized_b = prepared_b.confirm().await?;
    let finalized_a = prepared_a.confirm().await?;
    assert_eq!(finalized_b.state(), CashuMeltState::Paid);
    assert_eq!(finalized_a.state(), CashuMeltState::Paid);
    let out_of_order_b_proof = hex::encode(out_of_order_b_preimage);
    let out_of_order_a_proof = hex::encode(out_of_order_a_preimage);
    assert_eq!(
        finalized_b.payment_proof(),
        Some(out_of_order_b_proof.as_str())
    );
    assert_eq!(
        finalized_a.payment_proof(),
        Some(out_of_order_a_proof.as_str())
    );

    // A terminal Bark payment failure must propagate through the processor
    // event stream so the mint compensates the saga and releases every proof.
    let failure_quote = wallet
        .melt_quote(
            PaymentMethod::BOLT11,
            unreachable_invoice(5_000, 44).to_string(),
            None,
            None,
        )
        .await?;
    let before_failure = wallet.total_balance().await?;
    let prepared = wallet
        .prepare_melt(&failure_quote.id, Default::default())
        .await?;
    let failed_melt = tokio::time::timeout(NETWORK_TIMEOUT, prepared.confirm())
        .await
        .context("failed Cashu melt did not become terminal")?;
    assert!(failed_melt.is_err());
    let failed_quote = eventually("failed Cashu melt quote", NETWORK_TIMEOUT, || async {
        let quote = wallet.check_melt_quote_status(&failure_quote.id).await?;
        Ok((quote.state == CashuMeltState::Unpaid).then_some(quote))
    })
    .await?;
    assert!(failed_quote.payment_proof.is_none());
    eventually(
        "failed Cashu melt proof restoration",
        NETWORK_TIMEOUT,
        || async {
            let balance = wallet.total_balance().await?;
            Ok((balance == before_failure).then_some(()))
        },
    )
    .await?;

    mint.stop().await?;
    Ok(process)
}

async fn one_block_reorg_scenario(ctx: &TestContext, backend: &BarkBackend) -> Result<()> {
    let quote_id = QuoteId::new();
    let request = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Onchain(
            OnchainIncomingPaymentOptions {
                quote_id: quote_id.clone(),
            },
        ))
        .await?;
    ctx.bitcoind()
        .fund_addr(&request.request, sat(50_000))
        .await;
    ctx.generate_blocks(1).await;
    let identifier = PaymentIdentifier::QuoteId(quote_id);
    assert!(backend
        .check_incoming_payment_status(&identifier)
        .await?
        .is_empty());
    let invalidated = ctx.bitcoind().sync_client().get_best_block_hash()?;
    ctx.bitcoind()
        .sync_client()
        .invalidate_block(&invalidated)?;
    let mining_address = ctx.bitcoind().get_new_address();
    ctx.bitcoind()
        .sync_client()
        .call::<serde_json::Value>(
            "generateblock",
            &[mining_address.to_string().into(), serde_json::json!([])],
        )
        .context("mine an alternative block without the reorged deposit")?;
    ctx.await_block_count_sync().await;

    // The deposit existed only in the invalidated block and is no longer
    // confirmed on the active chain. It must not become a payable mint credit.
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let payments = backend.check_incoming_payment_status(&identifier).await?;
            if !payments.is_empty() {
                bail!("reorged deposit was credited")
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    })
    .await;
    assert!(
        result.is_err(),
        "reorg observation window ended unexpectedly"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the pinned Bark Regtest service shell; run `just regtest`"]
async fn bark_regtest_suite() -> Result<()> {
    // The upstream harness reads these once while constructing TestContext.
    std::env::set_var("CHAIN_SOURCE", "esplora");
    std::env::set_var("KEEP_ALL_TEST_DATA", "1");
    if std::env::var_os("TEST_DIRECTORY").is_none() {
        std::env::set_var(
            "TEST_DIRECTORY",
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/bark-regtest"),
        );
    }

    let ctx = fresh_test_context("cdk-payment-processor/bark-regtest").await;
    let lightning = ctx.new_lightning_setup("lightning").await;
    let ark_port = ark_testing::ports::pick_port();
    let ark_socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ark_port);
    let server = ctx
        .new_server_with_cfg("server", Some(&lightning.internal), move |cfg| {
            cfg.rpc.public_address = ark_socket;
            cfg.invoice_check_interval = POLL_INTERVAL;
            cfg.invoice_poll_interval = POLL_INTERVAL;
            cfg.invoice_expiry = Duration::from_secs(10);
            cfg.fees.lightning_send.base_fee = sat(5);
            // This suite performs several Lightning receives without a chain
            // tip change. Keep enough entries of each configured size that
            // pool change reaching its exit-depth limit cannot starve later
            // black-box and Cashu receive scenarios.
            for target in &mut cfg.vtxopool.vtxo_targets {
                target.count = target.count.max(8);
            }
        })
        .await;
    let server_address = server.new_onchain_address().await?;
    ctx.bitcoind().fund_addr(server_address, btc(20)).await;
    ctx.generate_blocks(1).await;
    eventually("Ark server chain sync", NETWORK_TIMEOUT, || async {
        let expected = ctx.bitcoind().get_block_count().await as u32;
        Ok((server.chain_tip().height >= expected).then_some(()))
    })
    .await?;
    eventually("Ark server VTXO pool", NETWORK_TIMEOUT, || async {
        let mut pool = Box::pin(server.database().load_vtxopool().await?);
        Ok(pool.next().await.transpose()?.map(|_| ()))
    })
    .await
    .context("initialize funded Ark server")?;

    let ark_url = format!("http://{ark_socket}");
    let esplora_url = ctx
        .electrs
        .as_ref()
        .context("Esplora is required")?
        .rest_url();
    let processor_data = ctx.datadir.join("processor-wallet");
    let config = backend_config(&processor_data, MNEMONIC_A, &ark_url, &esplora_url);
    let empty_send_address = ctx.bitcoind().get_new_address().to_string();
    let insufficient_invoice = Bolt11Invoice::from_str(
        &lightning
            .external
            .invoice(
                Some(sat(50_000)),
                format!("empty-wallet-{}", QuoteId::new()),
                "empty Bark wallet",
            )
            .await,
    )?;
    let logs = ctx.datadir.join("artifacts");

    let mut backend = wallet_lifecycle(
        &config,
        &ctx.datadir.join("other-wallet"),
        &empty_send_address,
        insufficient_invoice,
        &logs,
    )
    .await
    .context("wallet lifecycle")?;
    backend = onchain_scenarios(&ctx, &config, backend)
        .await
        .context("on-chain mint/melt and restart")?;
    lightning_scenarios(&lightning, &backend)
        .await
        .context("Lightning receive/send, fees, events, and idempotency")?;
    backend = stopped_receive_scenario(&lightning, &config, backend)
        .await
        .context("receive paid while stopped")?;
    backend = pending_send_restart_scenario(&lightning, &config, backend)
        .await
        .context("pending Lightning send across restart")?;
    backend = arkoor_scenario(&ctx, &ark_url, &config, backend)
        .await
        .context("arkoor custom payment and restart")?;
    drop(backend);

    let process = grpc_restart_scenario(&lightning, &config, &logs)
        .await
        .context("black-box gRPC process and restart")?;
    let mut process = cashu_scenario(&lightning, &config, &logs, process)
        .await
        .context("full Cashu mint/melt")?;
    process.stop().await?;

    // Reorging changes the shared chain tip, so leave this disruptive scenario
    // until all other services and black-box checks have completed.
    let backend = BarkBackend::new(&config).await?;
    one_block_reorg_scenario(&ctx, &backend)
        .await
        .context("one-block reorg")?;

    Ok(())
}
