use std::future::Future;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use bitcoin::hashes::{sha256, Hash};
use bitcoin::secp256k1::{Secp256k1, SecretKey};
use cdk_common::amount::Amount;
use cdk_common::payment::{
    Bolt11IncomingPaymentOptions, Bolt11OutgoingPaymentOptions, IncomingPaymentOptions,
    MintPayment, OutgoingPaymentOptions,
};
use cdk_common::{CurrencyUnit, QuoteId};
use cdk_payment_processor::PaymentProcessorClient;
use cdk_payment_processor_spark::backend::SparkBackend;
use cdk_payment_processor_spark::settings::{BackendConfig, OperatorSettings, SspSettings};
use futures::StreamExt;
use lightning_invoice::{Bolt11Invoice, Currency, InvoiceBuilder, PaymentSecret};
use spark_itest::fixtures::setup::TestFixtures;
use spark_wallet::{DefaultSigner, Network, SparkSigner, SparkSignerAdapter, WalletBuilder};
use tokio::process::{Child as TokioChild, Command as TokioCommand};
use tokio::time::Instant;

const POLL_INTERVAL: Duration = Duration::from_millis(200);
const NETWORK_TIMEOUT: Duration = Duration::from_secs(120);
const MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
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

fn unreachable_invoice(amount_sat: u64, marker: u8) -> Bolt11Invoice {
    let key = SecretKey::from_slice(&[marker; 32]).expect("valid secret key");
    InvoiceBuilder::new(Currency::Regtest)
        .description("unreachable regtest invoice".into())
        .payment_hash(sha256::Hash::from_slice(&[marker.saturating_add(1); 32]).expect("hash"))
        .payment_secret(PaymentSecret([marker.saturating_add(2); 32]))
        .amount_milli_satoshis(amount_sat * 1_000)
        .duration_since_epoch(Duration::from_secs(unix_now()))
        .expiry_time(Duration::from_secs(120))
        .min_final_cltv_expiry_delta(18)
        .build_signed(|hash| Secp256k1::new().sign_ecdsa_recoverable(hash, &key))
        .expect("build unreachable invoice")
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

/// Builds processor configuration pointing at the local federation. The CA
/// certificate of the shared operator certificate is written next to the run
/// so `ca_cert_path` exercises the real file-loading path.
fn backend_config(
    fixtures: &TestFixtures,
    mnemonic: &str,
    data_dir: &Path,
    cert_dir: &Path,
) -> Result<BackendConfig> {
    std::fs::create_dir_all(cert_dir)?;
    let mut operators = Vec::new();
    for operator in &fixtures.spark_so.operators {
        let ca_cert_path = cert_dir.join(format!("operator-{}-ca.pem", operator.index));
        std::fs::write(&ca_cert_path, &operator.ca_cert)?;
        operators.push(OperatorSettings {
            address: format!("https://127.0.0.1:{}", operator.host_port),
            identifier: format!("{:0>64}", operator.index + 1),
            identity_public_key: operator.public_key.to_string(),
            ca_cert_path: Some(ca_cert_path.to_string_lossy().into_owned()),
        });
    }

    Ok(BackendConfig {
        mnemonic: mnemonic.to_string(),
        network: Network::Regtest,
        data_dir: data_dir.to_string_lossy().into_owned(),
        operators,
        split_secret_threshold: None,
        ssp: None,
    })
}

async fn invalid_config_scenario(mnemonic: &str) -> Result<()> {
    // A signing threshold without operators cannot be resolved.
    let no_operators = BackendConfig {
        mnemonic: mnemonic.to_string(),
        network: Network::Regtest,
        split_secret_threshold: Some(2),
        ..BackendConfig::default()
    };
    assert!(
        SparkBackend::new(no_operators).await.is_err(),
        "split_secret_threshold without operators must be rejected"
    );

    // A threshold above the operator count is unsatisfiable.
    let single_operator = BackendConfig {
        mnemonic: mnemonic.to_string(),
        network: Network::Regtest,
        operators: vec![OperatorSettings {
            address: "https://127.0.0.1:8535".to_string(),
            identifier: format!("{:0>64}", 1),
            identity_public_key:
                "03dfbdff4b6332c220f8fa2ba8ed496c698ceada563fa01b67d9983bfc5c95e763".to_string(),
            ca_cert_path: None,
        }],
        split_secret_threshold: Some(2),
        ..BackendConfig::default()
    };
    assert!(
        SparkBackend::new(single_operator).await.is_err(),
        "threshold above operator count must be rejected"
    );
    Ok(())
}

async fn settings_scenario(backend: &SparkBackend) -> Result<()> {
    backend.start().await.context("backend start")?;
    let settings = backend.get_settings().await?;
    assert_eq!(settings.unit, "sat");
    let bolt11 = settings.bolt11.context("BOLT11 settings missing")?;
    assert!(!bolt11.mpp);
    assert!(!bolt11.amountless);
    assert!(!bolt11.invoice_description);
    assert!(settings.bolt12.is_none());
    assert!(settings.onchain.is_none());
    assert!(settings.custom.is_empty());
    Ok(())
}

async fn event_stream_lifecycle_scenario(backend: &SparkBackend) -> Result<()> {
    let mut stream = backend.wait_payment_event().await?;
    assert!(
        backend.is_payment_event_stream_active(),
        "event stream must report active after subscription"
    );
    // An idle federation must not emit any payment events.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .is_err(),
        "no payment events are expected on an idle federation"
    );
    backend.cancel_payment_event_stream();
    // Cancellation must terminate the stream rather than leave it hanging.
    let ended = tokio::time::timeout(Duration::from_secs(5), stream.next()).await;
    assert!(
        matches!(ended, Ok(None)),
        "stream must end after cancellation, got {ended:?}"
    );
    eventually(
        "event stream marked inactive",
        Duration::from_secs(5),
        || async { Ok((!backend.is_payment_event_stream_active()).then_some(())) },
    )
    .await?;
    Ok(())
}

/// The default configuration talks to the hosted Lightspark SSP, which is of
/// no use against a local federation. Pointing the processor at a dead SSP
/// through `backend.ssp` must make every Lightning swap fail cleanly instead
/// of hanging or reporting phantom success.
async fn dead_ssp_scenarios(fixtures: &TestFixtures, run_dir: &Path) -> Result<()> {
    let mut config = backend_config(
        fixtures,
        MNEMONIC,
        &run_dir.join("dead-ssp-data"),
        &run_dir.join("certs"),
    )?;
    config.ssp = Some(SspSettings {
        // TCP port 9 (discard) has no local listener: connection refused.
        base_url: "http://127.0.0.1:9".to_string(),
        identity_public_key: "022bf283544b16c0622daecb79422007d167eca6ce9f0c98c0c49833b1f7170bfe"
            .to_string(),
        schema_endpoint: None,
    });
    let backend = SparkBackend::new(config)
        .await
        .context("connect backend with dead SSP")?;

    let invoice = unreachable_invoice(5_000, 43);

    let quote = backend
        .get_payment_quote(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), QuoteId::new(), u64::MAX),
        )
        .await;
    assert!(
        quote.is_err(),
        "fee estimation without a usable SSP must fail"
    );

    let started = backend
        .make_payment(
            &CurrencyUnit::Sat,
            bolt11_options(invoice.clone(), QuoteId::new(), 1_000),
        )
        .await;
    match started {
        Err(_) => {}
        Ok(response) => assert_ne!(
            response.status,
            cdk_common::MeltQuoteState::Paid,
            "a payment without a usable SSP must never be reported as paid"
        ),
    }

    let incoming = backend
        .create_incoming_payment_request(IncomingPaymentOptions::Bolt11(
            Bolt11IncomingPaymentOptions {
                description: Some("no ssp".to_string()),
                amount: Amount::new(5_000, CurrencyUnit::Sat),
                unix_expiry: Some(unix_now() + 300),
            },
        ))
        .await;
    assert!(
        incoming.is_err(),
        "invoice creation without a usable SSP must fail"
    );
    Ok(())
}

/// Proves the federation is chain-integrated: a harness wallet deposits
/// on-chain funds and claims them into Spark leaves.
async fn deposit_scenario(fixtures: &TestFixtures) -> Result<()> {
    let seed = [9_u8; 32];
    let signer = DefaultSigner::new(&seed, Network::Regtest)?;
    let spark_signer: Arc<dyn SparkSigner> = Arc::new(SparkSignerAdapter::new(Arc::new(signer)));
    let wallet = WalletBuilder::new(fixtures.create_wallet_config().await?, spark_signer)
        .build()
        .await
        .context("build harness wallet")?;

    let deposit = wallet.generate_deposit_address().await?;
    let amount = bitcoin::Amount::from_sat(50_000);
    let txid = fixtures
        .bitcoind
        .fund_address(&deposit.address, amount)
        .await
        .context("fund deposit address")?;
    fixtures.bitcoind.generate_blocks(1).await?;
    fixtures.bitcoind.wait_for_tx_confirmation(&txid, 1).await?;
    fixtures
        .spark_so
        .wait_for_log("tree not found in available or creating status")
        .await?;

    let tx = fixtures.bitcoind.get_transaction(&txid).await?;
    let mut output_index = None;
    for (vout, output) in tx.output.iter().enumerate() {
        let Ok(address) =
            bitcoin::Address::from_script(&output.script_pubkey, bitcoin::Network::Regtest)
        else {
            continue;
        };
        if address == deposit.address {
            output_index = Some(vout as u32);
            break;
        }
    }
    let vout = output_index.context("deposit address missing from funding tx outputs")?;
    wallet.claim_deposit(tx, vout).await?;

    eventually("claimed deposit balance", NETWORK_TIMEOUT, || async {
        let balance = wallet.get_balance().await.map_err(anyhow::Error::from)?;
        Ok((balance == 50_000).then_some(()))
    })
    .await?;
    Ok(())
}

struct ProcessorProcess {
    child: TokioChild,
    port: u16,
}

impl ProcessorProcess {
    async fn spawn(run_dir: &Path, port: u16, config_toml: &str, log_id: u64) -> Result<Self> {
        std::fs::create_dir_all(run_dir)?;
        std::fs::write(run_dir.join("config.toml"), config_toml)?;
        let stdout =
            std::fs::File::create(run_dir.join(format!("processor-{port}-{log_id}.out.log")))?;
        let stderr =
            std::fs::File::create(run_dir.join(format!("processor-{port}-{log_id}.err.log")))?;
        let child = TokioCommand::new(env!("CARGO_BIN_EXE_cdk-payment-processor-spark"))
            .current_dir(run_dir)
            .env("SERVER_ADDRESS", "127.0.0.1")
            .env("SERVER_PORT", port.to_string())
            .env("ALLOW_INSECURE", "true")
            .env("RUST_LOG", "debug")
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .context("spawn cdk-payment-processor-spark")?;
        Ok(Self { child, port })
    }

    async fn client(&mut self) -> Result<PaymentProcessorClient> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match PaymentProcessorClient::new("http://127.0.0.1", self.port, None).await {
                Ok(client) => return Ok(client),
                Err(_) => {
                    if let Ok(Some(status)) = self.child.try_wait() {
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

async fn black_box_processor_scenario(
    fixtures: &TestFixtures,
    run_dir: &Path,
    logs: &Path,
) -> Result<()> {
    let process_dir = run_dir.join("processor");
    let data_dir = process_dir.join("data");
    let cert_dir = process_dir.join("certs");

    let mut config = backend_config(fixtures, MNEMONIC, &data_dir, &cert_dir)?;
    config.data_dir = data_dir.to_string_lossy().into_owned();

    let data_dir_str = data_dir.to_string_lossy().into_owned();
    let mut toml = format!(
        "[backend]\nmnemonic = \"{MNEMONIC}\"\nnetwork = \"regtest\"\ndata_dir = \"{data_dir_str}\"\n"
    );
    for operator in &config.operators {
        toml.push_str(&format!(
            "\n[[backend.operators]]\naddress = \"{}\"\nidentifier = \"{}\"\nidentity_public_key = \"{}\"\n",
            operator.address, operator.identifier, operator.identity_public_key
        ));
        if let Some(path) = &operator.ca_cert_path {
            toml.push_str(&format!("ca_cert_path = \"{}\"\n", path));
        }
    }

    let port = pick_port();
    let log_id = NEXT_PROCESS_LOG_ID.fetch_add(1, Ordering::Relaxed);
    let mut process = ProcessorProcess::spawn(&process_dir, port, &toml, log_id).await?;
    let client = process.client().await?;

    let settings = client.get_settings().await.context("gRPC get_settings")?;
    assert_eq!(settings.unit, "sat");
    assert!(settings.bolt11.is_some());
    assert!(settings.onchain.is_none());

    process.stop().await?;
    let restarted_log_id = NEXT_PROCESS_LOG_ID.fetch_add(1, Ordering::Relaxed);
    process = ProcessorProcess::spawn(&process_dir, port, &toml, restarted_log_id).await?;
    let client = process.client().await?;
    let settings = client
        .get_settings()
        .await
        .context("restart get_settings")?;
    assert_eq!(settings.unit, "sat");

    let _ = logs;
    process.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and builds the local spark operator images; run `just test-regtest`"]
async fn spark_regtest_suite() -> Result<()> {
    let root = std::env::var("TEST_DIRECTORY")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/spark-regtest")
        });
    let run_dir = root.join(format!("run-{}", unix_now()));
    std::fs::create_dir_all(run_dir.join("logs"))?;

    invalid_config_scenario(MNEMONIC)
        .await
        .context("invalid config rejection")?;

    eprintln!("Starting local Spark federation (bitcoind + 3 operators)...");
    let fixtures = TestFixtures::new()
        .await
        .context("start spark federation")?;
    eprintln!("Federation ready");

    deposit_scenario(&fixtures)
        .await
        .context("on-chain deposit claim")?;

    let data_dir = run_dir.join("processor-data");
    let backend = SparkBackend::new(backend_config(
        &fixtures,
        MNEMONIC,
        &data_dir,
        &run_dir.join("certs"),
    )?)
    .await
    .context("connect processor backend")?;

    settings_scenario(&backend)
        .await
        .context("settings scenario")?;
    event_stream_lifecycle_scenario(&backend)
        .await
        .context("event stream lifecycle")?;
    dead_ssp_scenarios(&fixtures, &run_dir)
        .await
        .context("dead SSP failure paths")?;

    let logs = run_dir.join("logs");
    black_box_processor_scenario(&fixtures, &run_dir, &logs)
        .await
        .context("black-box processor process")?;

    eprintln!("regtest artifacts kept in {}", run_dir.display());
    Ok(())
}
