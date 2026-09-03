use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ark::lightning::PaymentHash;
use ark::VtxoId;
use async_trait::async_trait;
use bark::onchain::bdk_wallet::TxOrdering;
use bark::onchain::{OnchainWallet, OnchainWalletTrait};
use bark::persist::sqlite::SqliteClient;
use bark::persist::BarkPersister;
use bitcoin::{Address, FeeRate, OutPoint, Psbt, Txid};
use cdk_common::amount::Amount;
use cdk_common::nuts::nut30::MeltQuoteOnchainFeeOption;
use cdk_common::nuts::CurrencyUnit;
use cdk_common::payment::{
    Bolt11Settings, CreateIncomingPaymentResponse, Event, IncomingPaymentOptions,
    MakePaymentResponse, MintPayment, OnchainSettings, OutgoingPaymentOptions, PaymentIdentifier,
    PaymentQuoteResponse, SettingsResponse, WaitPaymentResponse,
};
use cdk_common::{MeltQuoteState, QuoteId};
use futures::stream::{self, Stream, StreamExt};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::settings::{AdvertisedMethod, BackendConfig};

const ONCHAIN_CONFIRMATIONS: u32 = 1;
const ONCHAIN_FEE_INDEX: u32 = 0;
const ONCHAIN_ESTIMATED_BLOCKS: u32 = 6;

/// Bark payment processor backend using the Bark wallet library
#[derive(Clone)]
pub struct BarkBackend {
    wallet: Arc<bark::Wallet>,
    onchain_wallet: Arc<tokio::sync::RwLock<OnchainWallet>>,
    onchain_receive_lock: Arc<tokio::sync::Mutex<()>>,
    onchain_send_lock: Arc<tokio::sync::Mutex<()>>,
    lightning_send_lock: Arc<tokio::sync::Mutex<()>>,
    arkoor_send_lock: Arc<tokio::sync::Mutex<()>>,
    state_store: Arc<BarkStateStore>,
    network: bitcoin::Network,
    event_poll_interval: Duration,
    wait_invoice_active: Arc<AtomicBool>,
    /// Rails this backend reports in `get_settings`, and therefore the only
    /// ones a mint will register it for.
    advertised_methods: Vec<AdvertisedMethod>,
}

const RECEIVE_ADDRESSES_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("receive_addresses");
const RECEIVE_INTENTS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("receive_intents");
const REPORTED_RECEIVES_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("reported_receives");
const LIGHTNING_RECEIVE_QUOTES_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("lightning_receive_quotes");
const REPORTED_LIGHTNING_RECEIVES_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("reported_lightning_receives");
const SEND_INTENTS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("send_intents");
const COMPLETED_SENDS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("completed_sends");
const LIGHTNING_SEND_INTENTS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("lightning_send_intents");
const COMPLETED_LIGHTNING_SENDS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("completed_lightning_sends");
const ARKOOR_SEND_INTENTS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("arkoor_send_intents");
const ARKOOR_QUOTES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("arkoor_quotes");
const COMPLETED_ARKOOR_SENDS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("completed_arkoor_sends");
// Per-scan rotation cursors so capped scans resume where the last scan
// stopped instead of restarting at the first key every tick.
const SCAN_CURSOR_TABLE: TableDefinition<&str, &str> = TableDefinition::new("scan_cursors");
const RECEIVE_SCAN_CURSOR_KEY: &str = "receive_scan";
const LIGHTNING_RECEIVE_SCAN_CURSOR_KEY: &str = "lightning_receive_scan";
const ONCHAIN_SEND_SCAN_CURSOR_KEY: &str = "onchain_send_scan";
const LIGHTNING_SEND_SCAN_CURSOR_KEY: &str = "lightning_send_scan";
const ONCHAIN_SEND_RECONCILE_CURSOR_KEY: &str = "onchain_send_reconcile";
const LIGHTNING_SEND_RECONCILE_CURSOR_KEY: &str = "lightning_send_reconcile";
const ARKOOR_SEND_SCAN_CURSOR_KEY: &str = "arkoor_send_scan";

const RETRY_BACKOFF_SECS: u64 = 30;
const SEND_ATTEMPT_REVIEW_SECS: u64 = 60;
// Bound the number of intents reconciled per event-stream tick so a large
// backlog cannot starve the other event kinds indefinitely.
const MAX_INTENTS_RECONCILED_PER_TICK: usize = 32;
// Bound the number of records scanned while looking for one event so a large
// reported backlog cannot starve unreported entries indefinitely.
const MAX_RECEIVE_INTENTS_SCANNED_PER_TICK: usize = 64;
const MAX_SEND_INTENTS_SCANNED_PER_TICK: usize = 64;
const MAX_RECEIVE_QUOTES_SCANNED_PER_TICK: usize = 64;
const ARKOOR_PAYMENT_METHOD: &str = "arkoor";

/// Build the settings a mint reads to decide which rails to register this
/// backend for.
///
/// A mint registers a backend per `(unit, method)` pair and claims every method
/// the backend advertises here, so restricting `methods` is what allows bark to
/// sit alongside another backend instead of competing with it.
fn settings_response(methods: &[AdvertisedMethod]) -> SettingsResponse {
    let advertises = |method: AdvertisedMethod| methods.contains(&method);

    let custom = if advertises(AdvertisedMethod::Arkoor) {
        HashMap::from([(
            ARKOOR_PAYMENT_METHOD.to_string(),
            serde_json::json!({
                "request": "ark_address",
                "extra": {"amount_sat": "positive_integer"},
                "fee": "zero"
            })
            .to_string(),
        )])
    } else {
        HashMap::new()
    };

    SettingsResponse {
        unit: "sat".to_string(),
        bolt11: advertises(AdvertisedMethod::Bolt11).then_some(Bolt11Settings {
            mpp: false,
            amountless: false,
            invoice_description: true,
        }),
        bolt12: None,
        onchain: advertises(AdvertisedMethod::Onchain).then_some(OnchainSettings {
            confirmations: ONCHAIN_CONFIRMATIONS,
            min_receive_amount_sat: 1,
            min_send_amount_sat: 1,
        }),
        custom,
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OnchainReceiveIntentRecord {
    quote_id: String,
    deposit_outpoint: String,
    gross_sat: u64,
    state: OnchainReceiveIntentState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum OnchainReceiveIntentState {
    Detected {
        detected_at: u64,
    },
    BoardPreparing {
        attempt: u32,
        started_at: u64,
    },
    Boarding {
        attempt: u32,
        board_txid: String,
        board_vtxo_ids: Vec<String>,
        fee_sat: u64,
        amount_sat: u64,
        started_at: u64,
    },
    RetryableFailed {
        attempt: u32,
        reason: String,
        failed_at: u64,
        retry_after: u64,
    },
    NeedsReview {
        reason: String,
        failed_at: u64,
    },
    Finalized {
        board_txid: String,
        board_vtxo_ids: Vec<String>,
        fee_sat: u64,
        amount_sat: u64,
        finalized_at: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct OnchainSendIntentRecord {
    quote_id: String,
    address: String,
    amount_sat: u64,
    state: OnchainSendIntentState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum OnchainSendIntentState {
    Attempting {
        attempt: u32,
        attempt_id: String,
        fee_sat: u64,
        started_at: u64,
    },
    Broadcast {
        txid: String,
        fee_sat: u64,
        broadcast_at: u64,
    },
    NeedsReview {
        reason: String,
        fee_sat: Option<u64>,
        failed_at: u64,
    },
    Confirmed {
        txid: String,
        fee_sat: u64,
        confirmed_at: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LightningSendIntentRecord {
    quote_id: String,
    payment_hash: String,
    invoice: String,
    amount_sat: u64,
    #[serde(alias = "estimated_fee_sat")]
    preflight_fee_sat: u64,
    #[serde(default)]
    max_fee_sat: Option<u64>,
    #[serde(default)]
    fee_reconciled: bool,
    state: LightningSendIntentState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ArkoorQuoteRecord {
    request: String,
    amount_sat: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum LightningSendIntentState {
    Attempting {
        attempt: u32,
        attempt_id: String,
        started_at: u64,
    },
    Pending {
        fee_sat: u64,
        started_at: u64,
    },
    Paid {
        fee_sat: u64,
        preimage: String,
        paid_at: u64,
    },
    Failed {
        reason: String,
        fee_sat: Option<u64>,
        failed_at: u64,
    },
    NeedsReview {
        reason: String,
        failed_at: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ArkoorSendIntentRecord {
    quote_id: String,
    address: String,
    amount_sat: u64,
    successful_payments_before: usize,
    state: ArkoorSendIntentState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum ArkoorSendIntentState {
    Attempting { attempt_id: String, started_at: u64 },
    Paid { payment_proof: String, paid_at: u64 },
    NeedsReview { reason: String, failed_at: u64 },
}

struct BarkStateStore {
    db: Database,
}

impl BarkStateStore {
    fn open(path: PathBuf) -> anyhow::Result<Self> {
        let db = Database::create(path)?;
        let store = Self { db };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> anyhow::Result<()> {
        let tx = self.db.begin_write()?;
        {
            tx.open_table(RECEIVE_ADDRESSES_TABLE)?;
            tx.open_table(RECEIVE_INTENTS_TABLE)?;
            tx.open_table(REPORTED_RECEIVES_TABLE)?;
            tx.open_table(LIGHTNING_RECEIVE_QUOTES_TABLE)?;
            tx.open_table(REPORTED_LIGHTNING_RECEIVES_TABLE)?;
            tx.open_table(SEND_INTENTS_TABLE)?;
            tx.open_table(COMPLETED_SENDS_TABLE)?;
            tx.open_table(LIGHTNING_SEND_INTENTS_TABLE)?;
            tx.open_table(COMPLETED_LIGHTNING_SENDS_TABLE)?;
            tx.open_table(ARKOOR_SEND_INTENTS_TABLE)?;
            tx.open_table(COMPLETED_ARKOOR_SENDS_TABLE)?;
            tx.open_table(SCAN_CURSOR_TABLE)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn store_error(e: impl std::fmt::Display) -> cdk_common::payment::Error {
        cdk_common::payment::Error::Custom(format!("Bark state store error: {}", e))
    }

    fn get_scan_cursor(&self, scan: &str) -> Result<Option<String>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(SCAN_CURSOR_TABLE)
            .map_err(Self::store_error)?;
        Ok(table
            .get(scan)
            .map_err(Self::store_error)?
            .map(|value| value.value().to_string()))
    }

    fn put_scan_cursor(&self, scan: &str, key: &str) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(SCAN_CURSOR_TABLE)
                .map_err(Self::store_error)?;
            table.insert(scan, key).map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    // Rotate `keys` so iteration starts at the first key after the stored
    // cursor, wrapping to the beginning when needed. The cursor need not
    // still exist among `keys`, which keeps filtered scans moving forward.
    fn rotated_window(
        &self,
        scan: &str,
        keys: Vec<String>,
        max_scanned: usize,
    ) -> Result<Vec<String>, cdk_common::payment::Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let cursor = self.get_scan_cursor(scan)?;
        let start = cursor
            .as_ref()
            .and_then(|cursor| keys.iter().position(|key| key > cursor))
            .unwrap_or(0);
        let rotated: Vec<String> = keys
            .iter()
            .cycle()
            .skip(start)
            .take(keys.len().min(max_scanned))
            .cloned()
            .collect();
        Ok(rotated)
    }

    fn rotated_records<T>(
        &self,
        scan: &str,
        records: Vec<(String, T)>,
        max_scanned: usize,
    ) -> Result<Vec<(String, T)>, cdk_common::payment::Error> {
        let mut records_by_key: HashMap<String, T> = records.into_iter().collect();
        let mut keys: Vec<String> = records_by_key.keys().cloned().collect();
        keys.sort();

        let window = self.rotated_window(scan, keys, max_scanned)?;
        let mut rotated = Vec::with_capacity(window.len());
        for key in window {
            if let Some(record) = records_by_key.remove(&key) {
                rotated.push((key, record));
            }
        }
        Ok(rotated)
    }

    fn put_receive_address(
        &self,
        quote_id: &str,
        address: &str,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(RECEIVE_ADDRESSES_TABLE)
                .map_err(Self::store_error)?;
            table.insert(quote_id, address).map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn receive_addresses(&self) -> Result<HashMap<String, String>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(RECEIVE_ADDRESSES_TABLE)
            .map_err(Self::store_error)?;
        let mut addresses = HashMap::new();
        for entry in table.iter().map_err(Self::store_error)? {
            let (key, value) = entry.map_err(Self::store_error)?;
            addresses.insert(key.value().to_string(), value.value().to_string());
        }
        Ok(addresses)
    }

    fn get_receive_intent(
        &self,
        outpoint: &str,
    ) -> Result<Option<OnchainReceiveIntentRecord>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(RECEIVE_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        table
            .get(outpoint)
            .map_err(Self::store_error)?
            .map(|value| serde_json::from_str(value.value()).map_err(Self::store_error))
            .transpose()
    }

    fn put_receive_intent(
        &self,
        intent: &OnchainReceiveIntentRecord,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(RECEIVE_INTENTS_TABLE)
                .map_err(Self::store_error)?;
            let value = serde_json::to_string(intent).map_err(Self::store_error)?;
            table
                .insert(intent.deposit_outpoint.as_str(), value.as_str())
                .map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn receive_intents(
        &self,
    ) -> Result<Vec<OnchainReceiveIntentRecord>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(RECEIVE_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        let mut intents = Vec::new();
        for entry in table.iter().map_err(Self::store_error)? {
            let (_, value) = entry.map_err(Self::store_error)?;
            intents.push(serde_json::from_str(value.value()).map_err(Self::store_error)?);
        }
        Ok(intents)
    }

    fn finalized_receives_for_quote(
        &self,
        quote_id: &str,
    ) -> Result<Vec<OnchainReceiveIntentRecord>, cdk_common::payment::Error> {
        Ok(self
            .receive_intents()?
            .into_iter()
            .filter(|intent| {
                intent.quote_id == quote_id
                    && matches!(intent.state, OnchainReceiveIntentState::Finalized { .. })
            })
            .collect())
    }

    // Scan for the next finalized, not-yet-reported receive, rotating the
    // start position each call so a reported backlog cannot hide unreported
    // records behind the `max_scanned` prefix. `advance` updates the stored
    // cursor to the key of the intent being returned.
    fn next_unreported_finalized_receive(
        &self,
        max_scanned: usize,
    ) -> Result<Option<OnchainReceiveIntentRecord>, cdk_common::payment::Error> {
        let intents = self.receive_intents()?;
        let by_outpoint: HashMap<String, &OnchainReceiveIntentRecord> = intents
            .iter()
            .map(|intent| (intent.deposit_outpoint.clone(), intent))
            .collect();
        let mut keys: Vec<String> = by_outpoint.keys().cloned().collect();
        keys.sort();
        if keys.is_empty() {
            return Ok(None);
        }

        let window = self.rotated_window(RECEIVE_SCAN_CURSOR_KEY, keys, max_scanned)?;
        for outpoint in &window {
            let intent = by_outpoint[outpoint];
            if matches!(intent.state, OnchainReceiveIntentState::Finalized { .. })
                && !self.is_receive_reported(outpoint)?
            {
                return Ok(Some(intent.clone()));
            }
        }
        // Nothing unreported in this window; remember where we stopped so the
        // next scan continues from here.
        if let Some(last) = window.last() {
            self.put_scan_cursor(RECEIVE_SCAN_CURSOR_KEY, last)?;
        }
        Ok(None)
    }

    fn advance_receive_scan_cursor(
        &self,
        outpoint: &str,
    ) -> Result<(), cdk_common::payment::Error> {
        self.put_scan_cursor(RECEIVE_SCAN_CURSOR_KEY, outpoint)
    }

    // Record that an onchain receive was delivered to the mint. The marker,
    // the finalized intent, and the quote's receive address are all kept: the
    // marker deduplicates event emission and caps the poll set, the intent
    // lets status re-checks keep reporting the payment to the mint, and the
    // address keeps detecting further deposits to the same amountless quote.
    fn mark_onchain_receive_reported(
        &self,
        outpoint: &str,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut reported = tx
                .open_table(REPORTED_RECEIVES_TABLE)
                .map_err(Self::store_error)?;
            reported.insert(outpoint, "1").map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn is_receive_reported(&self, outpoint: &str) -> Result<bool, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(REPORTED_RECEIVES_TABLE)
            .map_err(Self::store_error)?;
        Ok(table.get(outpoint).map_err(Self::store_error)?.is_some())
    }

    fn put_lightning_receive_quote(
        &self,
        quote_id: &str,
        payment_hash: &str,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(LIGHTNING_RECEIVE_QUOTES_TABLE)
                .map_err(Self::store_error)?;
            table
                .insert(quote_id, payment_hash)
                .map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn get_lightning_receive_hash(
        &self,
        quote_id: &str,
    ) -> Result<Option<String>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(LIGHTNING_RECEIVE_QUOTES_TABLE)
            .map_err(Self::store_error)?;
        Ok(table
            .get(quote_id)
            .map_err(Self::store_error)?
            .map(|value| value.value().to_string()))
    }

    fn lightning_receive_quotes(
        &self,
    ) -> Result<Vec<(String, String)>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(LIGHTNING_RECEIVE_QUOTES_TABLE)
            .map_err(Self::store_error)?;
        let mut quotes = Vec::new();
        for entry in table.iter().map_err(Self::store_error)? {
            let (quote_id, stored_hash) = entry.map_err(Self::store_error)?;
            quotes.push((
                quote_id.value().to_string(),
                stored_hash.value().to_string(),
            ));
        }
        Ok(quotes)
    }

    fn mark_lightning_receive_reported(
        &self,
        request_lookup_id: &str,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(REPORTED_LIGHTNING_RECEIVES_TABLE)
                .map_err(Self::store_error)?;
            table
                .insert(request_lookup_id, "1")
                .map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn is_lightning_receive_reported(
        &self,
        request_lookup_id: &str,
    ) -> Result<bool, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(REPORTED_LIGHTNING_RECEIVES_TABLE)
            .map_err(Self::store_error)?;
        Ok(table
            .get(request_lookup_id)
            .map_err(Self::store_error)?
            .is_some())
    }

    fn put_send(
        &self,
        quote_id: &str,
        send: &OnchainSendIntentRecord,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(SEND_INTENTS_TABLE)
                .map_err(Self::store_error)?;
            let value = serde_json::to_string(send).map_err(Self::store_error)?;
            table
                .insert(quote_id, value.as_str())
                .map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn get_send(
        &self,
        quote_id: &str,
    ) -> Result<Option<OnchainSendIntentRecord>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(SEND_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        table
            .get(quote_id)
            .map_err(Self::store_error)?
            .map(|value| serde_json::from_str(value.value()).map_err(Self::store_error))
            .transpose()
    }

    fn sends(&self) -> Result<Vec<(String, OnchainSendIntentRecord)>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(SEND_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        let mut sends = Vec::new();
        for entry in table.iter().map_err(Self::store_error)? {
            let (key, value) = entry.map_err(Self::store_error)?;
            sends.push((
                key.value().to_string(),
                serde_json::from_str(value.value()).map_err(Self::store_error)?,
            ));
        }
        Ok(sends)
    }

    fn mark_send_completed(&self, quote_id: &str) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(COMPLETED_SENDS_TABLE)
                .map_err(Self::store_error)?;
            table.insert(quote_id, "1").map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn is_send_completed(&self, quote_id: &str) -> Result<bool, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(COMPLETED_SENDS_TABLE)
            .map_err(Self::store_error)?;
        Ok(table.get(quote_id).map_err(Self::store_error)?.is_some())
    }

    fn put_lightning_send(
        &self,
        payment_hash: &str,
        send: &LightningSendIntentRecord,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(LIGHTNING_SEND_INTENTS_TABLE)
                .map_err(Self::store_error)?;
            let value = serde_json::to_string(send).map_err(Self::store_error)?;
            table
                .insert(payment_hash, value.as_str())
                .map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn get_lightning_send(
        &self,
        payment_hash: &str,
    ) -> Result<Option<LightningSendIntentRecord>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(LIGHTNING_SEND_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        table
            .get(payment_hash)
            .map_err(Self::store_error)?
            .map(|value| serde_json::from_str(value.value()).map_err(Self::store_error))
            .transpose()
    }

    fn lightning_sends(
        &self,
    ) -> Result<Vec<(String, LightningSendIntentRecord)>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(LIGHTNING_SEND_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        let mut sends = Vec::new();
        for entry in table.iter().map_err(Self::store_error)? {
            let (key, value) = entry.map_err(Self::store_error)?;
            sends.push((
                key.value().to_string(),
                serde_json::from_str(value.value()).map_err(Self::store_error)?,
            ));
        }
        Ok(sends)
    }

    fn lightning_send_for_quote(
        &self,
        quote_id: &str,
    ) -> Result<Option<(String, LightningSendIntentRecord)>, cdk_common::payment::Error> {
        Ok(self
            .lightning_sends()?
            .into_iter()
            .find(|(_, send)| send.quote_id == quote_id))
    }

    fn mark_lightning_send_completed(
        &self,
        payment_hash: &str,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(COMPLETED_LIGHTNING_SENDS_TABLE)
                .map_err(Self::store_error)?;
            table.insert(payment_hash, "1").map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn is_lightning_send_completed(
        &self,
        payment_hash: &str,
    ) -> Result<bool, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(COMPLETED_LIGHTNING_SENDS_TABLE)
            .map_err(Self::store_error)?;
        Ok(table
            .get(payment_hash)
            .map_err(Self::store_error)?
            .is_some())
    }

    fn put_arkoor_send(
        &self,
        quote_id: &str,
        send: &ArkoorSendIntentRecord,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(ARKOOR_SEND_INTENTS_TABLE)
                .map_err(Self::store_error)?;
            let value = serde_json::to_string(send).map_err(Self::store_error)?;
            table
                .insert(quote_id, value.as_str())
                .map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn put_arkoor_quote(
        &self,
        quote_id: &str,
        quote: &ArkoorQuoteRecord,
    ) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(ARKOOR_QUOTES_TABLE)
                .map_err(Self::store_error)?;
            let value = serde_json::to_string(quote).map_err(Self::store_error)?;
            table
                .insert(quote_id, value.as_str())
                .map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn get_arkoor_quote(
        &self,
        quote_id: &str,
    ) -> Result<Option<ArkoorQuoteRecord>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(ARKOOR_QUOTES_TABLE)
            .map_err(Self::store_error)?;
        table
            .get(quote_id)
            .map_err(Self::store_error)?
            .map(|value| serde_json::from_str(value.value()).map_err(Self::store_error))
            .transpose()
    }

    fn get_arkoor_send(
        &self,
        quote_id: &str,
    ) -> Result<Option<ArkoorSendIntentRecord>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(ARKOOR_SEND_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        table
            .get(quote_id)
            .map_err(Self::store_error)?
            .map(|value| serde_json::from_str(value.value()).map_err(Self::store_error))
            .transpose()
    }

    fn arkoor_sends(
        &self,
    ) -> Result<Vec<(String, ArkoorSendIntentRecord)>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(ARKOOR_SEND_INTENTS_TABLE)
            .map_err(Self::store_error)?;
        let mut sends = Vec::new();
        for entry in table.iter().map_err(Self::store_error)? {
            let (key, value) = entry.map_err(Self::store_error)?;
            sends.push((
                key.value().to_string(),
                serde_json::from_str(value.value()).map_err(Self::store_error)?,
            ));
        }
        Ok(sends)
    }

    fn mark_arkoor_send_completed(&self, quote_id: &str) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(COMPLETED_ARKOOR_SENDS_TABLE)
                .map_err(Self::store_error)?;
            table.insert(quote_id, "1").map_err(Self::store_error)?;
        }
        tx.commit().map_err(Self::store_error)
    }

    fn is_arkoor_send_completed(&self, quote_id: &str) -> Result<bool, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(COMPLETED_ARKOOR_SENDS_TABLE)
            .map_err(Self::store_error)?;
        Ok(table.get(quote_id).map_err(Self::store_error)?.is_some())
    }
}

// Build an unsigned board funding PSBT restricted to the given deposit UTXO.
// The output is a full drain of the selected UTXO; the board fee and miner fee
// are deducted from it.
fn build_board_funding_psbt(
    onchain: &mut OnchainWallet,
    outpoint: OutPoint,
    funding_address: &Address,
    fee_rate: FeeRate,
) -> anyhow::Result<Psbt> {
    let mut builder = onchain.build_tx();
    builder.ordering(TxOrdering::Untouched);
    builder.add_utxo(outpoint)?;
    builder.manually_selected_only();
    builder.drain_to(funding_address.script_pubkey());
    builder.fee_rate(fee_rate);
    builder.finish().map_err(Into::into)
}

impl BarkBackend {
    fn parse_network(network: &str) -> anyhow::Result<bitcoin::Network> {
        match network.to_ascii_lowercase().as_str() {
            "mainnet" => Ok(bitcoin::Network::Bitcoin),
            "testnet" => Ok(bitcoin::Network::Testnet),
            "signet" => Ok(bitcoin::Network::Signet),
            "regtest" => Ok(bitcoin::Network::Regtest),
            _ => anyhow::bail!(
                "Unsupported Bark network `{network}`; expected one of: mainnet, testnet, signet, regtest"
            ),
        }
    }

    /// Create a new Bark backend with initialized wallet
    pub async fn new(config: &BackendConfig) -> anyhow::Result<Self> {
        info!("Initializing Bark backend");

        if config.event_poll_interval_ms == 0 {
            anyhow::bail!("BARK_EVENT_POLL_INTERVAL_MS must be greater than zero");
        }

        let advertised_methods = config.advertised_methods()?;
        info!(
            "Advertising payment methods: {}",
            advertised_methods
                .iter()
                .map(|method| method.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Parse the mnemonic
        let mnemonic = config
            .mnemonic
            .parse::<bip39::Mnemonic>()
            .map_err(|e| anyhow::anyhow!("Invalid mnemonic: {}", e))?;

        let network = Self::parse_network(&config.network)?;

        // Build bark config
        let bark_config = bark::Config {
            server_address: config.server_address.clone(),
            esplora_address: Some(config.esplora_address.clone()),
            ..bark::Config::network_default(network)
        };

        // Create data directory if it doesn't exist
        let data_dir = PathBuf::from(&config.data_dir);
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| anyhow::anyhow!("Failed to create data directory: {}", e))?;

        // Open SQLite database
        let db_path = data_dir.join("db.sqlite");
        let db: Arc<dyn BarkPersister> = Arc::new(
            SqliteClient::open(&db_path)
                .map_err(|e| anyhow::anyhow!("Failed to open SQLite database: {}", e))?,
        );

        let onchain_wallet =
            OnchainWallet::load_or_create(network, mnemonic.to_seed(""), db.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load onchain wallet: {}", e))?;
        let onchain_wallet = Arc::new(tokio::sync::RwLock::new(onchain_wallet));
        let bark_onchain_wallet: Arc<tokio::sync::RwLock<dyn OnchainWalletTrait>> =
            onchain_wallet.clone();

        let wallet = bark::Wallet::open(
            network,
            bark::WalletSeed::new_from_mnemonic(network, &mnemonic),
            bark_config,
            bark::OpenWalletArgs {
                run_daemon: false,
                datadir: Some(data_dir.clone()),
                persister: Some(db),
                onchain: Some(bark_onchain_wallet),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open or create wallet: {}", e))?;

        onchain_wallet
            .write()
            .await
            .sync(wallet.chain())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to sync onchain wallet: {}", e))?;

        let state_store = Arc::new(
            BarkStateStore::open(data_dir.join("onchain_state.redb"))
                .map_err(|e| anyhow::anyhow!("Failed to open onchain state store: {}", e))?,
        );

        info!("Bark backend initialized successfully");
        match wallet.balance().await {
            Ok(balance) => info!("Current Bark balance: {} sats", balance.spendable.to_sat()),
            Err(e) => warn!("Failed to get current Bark balance: {}", e),
        }

        Ok(Self {
            wallet: Arc::new(wallet),
            onchain_wallet,
            onchain_receive_lock: Arc::new(tokio::sync::Mutex::new(())),
            onchain_send_lock: Arc::new(tokio::sync::Mutex::new(())),
            lightning_send_lock: Arc::new(tokio::sync::Mutex::new(())),
            arkoor_send_lock: Arc::new(tokio::sync::Mutex::new(())),
            state_store,
            network,
            event_poll_interval: Duration::from_millis(config.event_poll_interval_ms),
            wait_invoice_active: Arc::new(AtomicBool::new(false)),
            advertised_methods,
        })
    }

    /// Return the spendable Bark balance for the opt-in regtest harness.
    #[cfg(feature = "regtest-tests")]
    #[doc(hidden)]
    pub async fn regtest_spendable_balance_sat(&self) -> anyhow::Result<u64> {
        Ok(self.wallet.balance().await?.spendable.to_sat())
    }

    /// Derive and persist the next Ark address for lifecycle tests.
    #[cfg(feature = "regtest-tests")]
    #[doc(hidden)]
    pub async fn regtest_new_ark_address(&self) -> anyhow::Result<String> {
        Ok(self.wallet.new_address().await?.to_string())
    }

    /// Return a previously derived Ark address for lifecycle tests.
    #[cfg(feature = "regtest-tests")]
    #[doc(hidden)]
    pub async fn regtest_peek_ark_address(&self, index: u32) -> anyhow::Result<String> {
        Ok(self.wallet.peek_address(index).await?.to_string())
    }

    /// Count Bark movements for one Lightning payment hash. This lets the
    /// opt-in topology test verify that concurrent retries create one external
    /// payment, independently of the processor's cached response.
    #[cfg(feature = "regtest-tests")]
    #[doc(hidden)]
    pub async fn regtest_lightning_movement_count(
        &self,
        payment_hash: [u8; 32],
    ) -> anyhow::Result<usize> {
        let payment_hash = PaymentHash::from(payment_hash);
        Ok(self
            .wallet
            .history()
            .await?
            .into_iter()
            .filter(|movement| movement.lightning_payment_hash() == Some(payment_hash))
            .count())
    }

    fn parse_bitcoin_address(
        &self,
        address: &str,
    ) -> Result<bitcoin::Address, cdk_common::payment::Error> {
        address
            .parse::<bitcoin::Address<_>>()
            .map_err(|e| cdk_common::payment::Error::Custom(format!("Invalid address: {}", e)))?
            .require_network(self.network)
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Address network mismatch: {}", e))
            })
    }

    async fn process_onchain_receive_boards(&self) -> Result<(), cdk_common::payment::Error> {
        // Best effort: skip this round if another caller is already processing
        // so status checks do not queue behind a slow event-stream tick.
        let Ok(_receive_guard) = self.onchain_receive_lock.try_lock() else {
            return Ok(());
        };

        if let Err(e) = self.wallet.sync_pending_boards().await {
            debug!("Failed to sync pending boards: {}", e);
        }

        let tip = self.wallet.chain().tip().await.map_err(|e| {
            cdk_common::payment::Error::Custom(format!("Failed to get chain tip: {}", e))
        })?;

        let mut onchain = self.onchain_wallet.write().await;
        onchain.sync(self.wallet.chain()).await.map_err(|e| {
            cdk_common::payment::Error::Custom(format!("Failed to sync onchain wallet: {}", e))
        })?;

        self.recover_preparing_receive_boards(&onchain).await?;
        self.finalize_spendable_receive_boards().await?;
        self.detect_confirmed_receive_deposits(&onchain, tip)
            .await?;
        drop(onchain);

        self.start_ready_receive_boards().await
    }

    async fn detect_confirmed_receive_deposits(
        &self,
        onchain: &OnchainWallet,
        tip: u32,
    ) -> Result<(), cdk_common::payment::Error> {
        let receive_addresses = self.state_store.receive_addresses()?;
        if receive_addresses.is_empty() {
            return Ok(());
        }
        let address_to_quote = receive_addresses
            .iter()
            .map(|(quote_id, address)| (address.clone(), quote_id.clone()))
            .collect::<HashMap<_, _>>();

        for output in onchain.list_unspent() {
            let Some(height) = output.chain_position.confirmation_height_upper_bound() else {
                continue;
            };
            let confirmations = tip.saturating_sub(height.saturating_sub(1));
            if confirmations < ONCHAIN_CONFIRMATIONS {
                continue;
            }

            let output_address =
                bitcoin::Address::from_script(output.txout.script_pubkey.as_script(), self.network)
                    .map(|addr| addr.to_string())
                    .ok();
            let Some(quote_id_str) = output_address
                .as_ref()
                .and_then(|address| address_to_quote.get(address))
            else {
                continue;
            };

            let outpoint = output.outpoint.to_string();
            if self.state_store.get_receive_intent(&outpoint)?.is_some() {
                continue;
            }

            if let Err(e) = QuoteId::from_str(quote_id_str) {
                warn!(
                    "Skipping onchain deposit for invalid stored quote id {}: {}",
                    quote_id_str, e
                );
                continue;
            }

            let intent = OnchainReceiveIntentRecord {
                quote_id: quote_id_str.clone(),
                deposit_outpoint: outpoint.clone(),
                gross_sat: output.txout.value.to_sat(),
                state: OnchainReceiveIntentState::Detected {
                    detected_at: Self::unix_now(),
                },
            };
            self.state_store.put_receive_intent(&intent)?;

            info!(
                "Detected confirmed onchain receive {} for quote {}: gross {} sat",
                outpoint, quote_id_str, intent.gross_sat
            );
        }

        Ok(())
    }

    async fn start_ready_receive_boards(&self) -> Result<(), cdk_common::payment::Error> {
        let now = Self::unix_now();
        for intent in self.state_store.receive_intents()? {
            let (attempt, ready) = match &intent.state {
                OnchainReceiveIntentState::Detected { .. } => (1, true),
                OnchainReceiveIntentState::RetryableFailed {
                    attempt,
                    retry_after,
                    ..
                } => (attempt.saturating_add(1), *retry_after <= now),
                _ => (0, false),
            };

            if !ready {
                continue;
            }

            let outpoint = match OutPoint::from_str(&intent.deposit_outpoint) {
                Ok(outpoint) => outpoint,
                Err(e) => {
                    warn!(
                        "Skipping receive intent with invalid stored deposit outpoint {}: {}",
                        intent.deposit_outpoint, e
                    );
                    continue;
                }
            };

            let started_at = Self::unix_now();
            let mut preparing = intent.clone();
            preparing.state = OnchainReceiveIntentState::BoardPreparing {
                attempt,
                started_at,
            };
            self.state_store.put_receive_intent(&preparing)?;

            let board_result = self.board_deposit(outpoint).await;
            let onchain = self.onchain_wallet.read().await;
            let target_still_unspent = onchain
                .list_unspent()
                .iter()
                .any(|output| output.outpoint == outpoint);
            drop(onchain);

            match board_result {
                Ok(pending_board) => {
                    let board_intent = Self::boarding_intent_from_pending(
                        preparing,
                        pending_board,
                        attempt,
                        started_at,
                    );
                    self.state_store.put_receive_intent(&board_intent)?;
                    if let OnchainReceiveIntentState::Boarding {
                        board_txid,
                        amount_sat,
                        ..
                    } = &board_intent.state
                    {
                        info!(
                            "Started board {} for onchain receive {} quote {}: gross {} sat, net {} sat",
                            board_txid,
                            board_intent.deposit_outpoint,
                            board_intent.quote_id,
                            board_intent.gross_sat,
                            amount_sat
                        );
                    }
                }
                Err(e) => {
                    let reason = e.to_string();
                    warn!(
                        "Failed to start board for onchain receive {} quote {}: {}",
                        preparing.deposit_outpoint, preparing.quote_id, reason
                    );

                    if let Some(pending_board) =
                        self.pending_board_spending_outpoint(outpoint).await?
                    {
                        let board_intent = Self::boarding_intent_from_pending(
                            preparing,
                            pending_board,
                            attempt,
                            started_at,
                        );
                        self.state_store.put_receive_intent(&board_intent)?;
                    } else if target_still_unspent {
                        let mut failed = preparing;
                        failed.state = OnchainReceiveIntentState::RetryableFailed {
                            attempt,
                            reason,
                            failed_at: Self::unix_now(),
                            retry_after: Self::unix_now().saturating_add(RETRY_BACKOFF_SECS),
                        };
                        self.state_store.put_receive_intent(&failed)?;
                    } else {
                        let mut needs_review = preparing;
                        needs_review.state = OnchainReceiveIntentState::NeedsReview {
                            reason: format!(
                                "Board attempt failed after target outpoint stopped being spendable: {}",
                                reason
                            ),
                            failed_at: Self::unix_now(),
                        };
                        self.state_store.put_receive_intent(&needs_review)?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn recover_preparing_receive_boards(
        &self,
        onchain: &OnchainWallet,
    ) -> Result<(), cdk_common::payment::Error> {
        for intent in self.state_store.receive_intents()? {
            let OnchainReceiveIntentState::BoardPreparing {
                attempt,
                started_at,
                ..
            } = intent.state
            else {
                continue;
            };

            let outpoint = match OutPoint::from_str(&intent.deposit_outpoint) {
                Ok(outpoint) => outpoint,
                Err(e) => {
                    warn!(
                        "Skipping receive intent with invalid stored deposit outpoint {}: {}",
                        intent.deposit_outpoint, e
                    );
                    continue;
                }
            };

            if let Some(pending_board) = self.pending_board_spending_outpoint(outpoint).await? {
                let recovered =
                    Self::boarding_intent_from_pending(intent, pending_board, attempt, started_at);
                self.state_store.put_receive_intent(&recovered)?;
            } else if onchain
                .list_unspent()
                .iter()
                .any(|output| output.outpoint == outpoint)
            {
                let mut retryable = intent;
                retryable.state = OnchainReceiveIntentState::RetryableFailed {
                    attempt,
                    reason: "Interrupted before board was committed".to_string(),
                    failed_at: Self::unix_now(),
                    retry_after: Self::unix_now(),
                };
                self.state_store.put_receive_intent(&retryable)?;
            } else {
                let mut needs_review = intent;
                needs_review.state = OnchainReceiveIntentState::NeedsReview {
                    reason: "Interrupted board attempt spent the target outpoint but no Bark pending board was found".to_string(),
                    failed_at: Self::unix_now(),
                };
                self.state_store.put_receive_intent(&needs_review)?;
            }
        }

        Ok(())
    }

    async fn finalize_spendable_receive_boards(&self) -> Result<(), cdk_common::payment::Error> {
        'intents: for intent in self.state_store.receive_intents()? {
            let OnchainReceiveIntentState::Boarding {
                board_txid,
                board_vtxo_ids,
                fee_sat,
                amount_sat,
                ..
            } = &intent.state
            else {
                continue;
            };

            for vtxo_id in board_vtxo_ids {
                let vtxo_id = match VtxoId::from_str(vtxo_id) {
                    Ok(vtxo_id) => vtxo_id,
                    Err(e) => {
                        warn!("Invalid stored board vtxo id {}: {}", vtxo_id, e);
                        continue 'intents;
                    }
                };
                let vtxo = match self.wallet.get_vtxo_by_id(vtxo_id).await {
                    Ok(vtxo) => vtxo,
                    Err(e) => {
                        debug!("Board vtxo {} is not available yet: {}", vtxo_id, e);
                        continue 'intents;
                    }
                };

                if !matches!(vtxo.state.kind(), bark::vtxo::VtxoStateKind::Spendable) {
                    continue 'intents;
                }
            }

            let mut finalized = intent.clone();
            finalized.state = OnchainReceiveIntentState::Finalized {
                board_txid: board_txid.clone(),
                board_vtxo_ids: board_vtxo_ids.clone(),
                fee_sat: *fee_sat,
                amount_sat: *amount_sat,
                finalized_at: Self::unix_now(),
            };
            self.state_store.put_receive_intent(&finalized)?;

            info!(
                "Finalized onchain receive {} for quote {} after board {} became spendable",
                finalized.deposit_outpoint, finalized.quote_id, board_txid
            );
        }

        Ok(())
    }

    // Board a single confirmed deposit UTXO by building and signing the funding
    // transaction locally, then handing it to Bark via `board_psbt`. This keeps
    // per-quote accounting exact: only the selected outpoint is spent.
    async fn board_deposit(
        &self,
        outpoint: OutPoint,
    ) -> Result<bark::persist::models::PendingBoard, anyhow::Error> {
        let (user_keypair, _) = self.wallet.derive_store_next_keypair().await?;
        let (funding_address, expiry_height) =
            self.wallet.board_funding_address(&user_keypair).await?;
        let fee_rate = self.wallet.chain().fee_rates().await.regular;

        let signed_psbt = {
            let mut onchain = self.onchain_wallet.write().await;
            let psbt =
                build_board_funding_psbt(&mut onchain, outpoint, &funding_address, fee_rate)?;
            OnchainWalletTrait::finish_psbt(&mut *onchain, psbt).await?
        };

        self.wallet
            .board_psbt(signed_psbt, user_keypair, expiry_height)
            .await
    }

    async fn pending_board_spending_outpoint(
        &self,
        outpoint: OutPoint,
    ) -> Result<Option<bark::persist::models::PendingBoard>, cdk_common::payment::Error> {
        let pending_boards = self.wallet.pending_boards().await.map_err(|e| {
            cdk_common::payment::Error::Custom(format!("Failed to list pending boards: {}", e))
        })?;

        Ok(pending_boards.into_iter().find(|board| {
            board
                .funding_tx
                .input
                .iter()
                .any(|input| input.previous_output == outpoint)
        }))
    }

    fn boarding_intent_from_pending(
        mut intent: OnchainReceiveIntentRecord,
        pending_board: bark::persist::models::PendingBoard,
        attempt: u32,
        started_at: u64,
    ) -> OnchainReceiveIntentRecord {
        let amount_sat = pending_board.amount.to_sat();
        let fee_sat = intent.gross_sat.saturating_sub(amount_sat);
        intent.state = OnchainReceiveIntentState::Boarding {
            attempt,
            board_txid: pending_board.funding_tx.compute_txid().to_string(),
            board_vtxo_ids: pending_board
                .vtxos
                .iter()
                .map(ToString::to_string)
                .collect(),
            fee_sat,
            amount_sat,
            started_at,
        };
        intent
    }

    async fn check_onchain_receive(
        &self,
        quote_id: &QuoteId,
        mark_reported: bool,
    ) -> Result<Vec<WaitPaymentResponse>, cdk_common::payment::Error> {
        self.process_onchain_receive_boards().await?;

        let responses = self
            .state_store
            .finalized_receives_for_quote(&quote_id.to_string())?
            .into_iter()
            .filter_map(|receive| {
                let OnchainReceiveIntentState::Finalized {
                    board_txid,
                    amount_sat,
                    ..
                } = receive.state
                else {
                    return None;
                };

                Some((
                    receive.deposit_outpoint,
                    WaitPaymentResponse {
                        payment_identifier: PaymentIdentifier::QuoteId(quote_id.clone()),
                        payment_amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                        payment_id: board_txid,
                    },
                ))
            })
            .collect::<Vec<_>>();

        if mark_reported && !responses.is_empty() {
            for (outpoint, _) in &responses {
                self.state_store.mark_onchain_receive_reported(outpoint)?;
            }
        }

        Ok(responses
            .into_iter()
            .map(|(_, response)| response)
            .collect())
    }

    async fn next_onchain_receive_event(
        &self,
    ) -> Result<Option<Event>, cdk_common::payment::Error> {
        self.process_onchain_receive_boards().await?;

        let Some(receive) = self
            .state_store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)?
        else {
            return Ok(None);
        };

        let quote_id = match QuoteId::from_str(&receive.quote_id) {
            Ok(quote_id) => quote_id,
            Err(e) => {
                warn!(
                    "Skipping finalized receive with invalid stored quote id {}: {}",
                    receive.quote_id, e
                );
                return Ok(None);
            }
        };

        let OnchainReceiveIntentState::Finalized {
            board_txid,
            amount_sat,
            ..
        } = receive.state
        else {
            return Ok(None);
        };

        let response = WaitPaymentResponse {
            payment_identifier: PaymentIdentifier::QuoteId(quote_id),
            payment_amount: Amount::new(amount_sat, CurrencyUnit::Sat),
            payment_id: board_txid,
        };

        self.state_store
            .mark_onchain_receive_reported(&receive.deposit_outpoint)?;
        self.state_store
            .advance_receive_scan_cursor(&receive.deposit_outpoint)?;

        Ok(Some(Event::PaymentReceived(response)))
    }

    async fn next_onchain_send_event(&self) -> Result<Option<Event>, cdk_common::payment::Error> {
        self.reconcile_onchain_sends().await?;

        let sends = self.state_store.sends()?;
        let window = self.state_store.rotated_records(
            ONCHAIN_SEND_SCAN_CURSOR_KEY,
            sends,
            MAX_SEND_INTENTS_SCANNED_PER_TICK,
        )?;
        let mut last_scanned: Option<String> = None;
        for (quote_id_str, send) in window {
            last_scanned = Some(quote_id_str.clone());
            if self.state_store.is_send_completed(&quote_id_str)? {
                continue;
            }

            let OnchainSendIntentState::Confirmed { txid, fee_sat, .. } = &send.state else {
                continue;
            };

            let quote_id = match QuoteId::from_str(&quote_id_str) {
                Ok(quote_id) => quote_id,
                Err(e) => {
                    warn!(
                        "Skipping onchain send with invalid stored quote id {}: {}",
                        quote_id_str, e
                    );
                    continue;
                }
            };

            let total_spent = send.amount_sat.saturating_add(*fee_sat);
            let event = Event::PaymentSuccessful {
                quote_id: quote_id.clone(),
                details: MakePaymentResponse {
                    payment_lookup_id: PaymentIdentifier::QuoteId(quote_id),
                    payment_proof: Some(txid.clone()),
                    status: MeltQuoteState::Paid,
                    total_spent: Amount::new(total_spent, CurrencyUnit::Sat),
                },
            };
            self.state_store.mark_send_completed(&quote_id_str)?;
            self.state_store
                .put_scan_cursor(ONCHAIN_SEND_SCAN_CURSOR_KEY, &quote_id_str)?;
            return Ok(Some(event));
        }

        if let Some(last) = last_scanned {
            self.state_store
                .put_scan_cursor(ONCHAIN_SEND_SCAN_CURSOR_KEY, &last)?;
        }
        Ok(None)
    }

    async fn check_onchain_send(
        &self,
        quote_id: &QuoteId,
        mark_completed: bool,
    ) -> Result<Option<MakePaymentResponse>, cdk_common::payment::Error> {
        let quote_id_str = quote_id.to_string();
        let send = match self.onchain_send_lock.try_lock() {
            Ok(_send_guard) => match self.reconcile_onchain_send(&quote_id_str).await {
                Ok(send) => send,
                Err(e) => {
                    warn!(
                        "Failed to reconcile onchain send {} during status check: {}",
                        quote_id_str, e
                    );
                    self.state_store.get_send(&quote_id_str)?
                }
            },
            Err(_) => self.state_store.get_send(&quote_id_str)?,
        };
        let Some(send) = send else {
            return Ok(None);
        };

        Ok(Some(self.onchain_send_response(
            quote_id,
            &send,
            mark_completed,
        )?))
    }

    fn onchain_send_needs_reconciliation(send: &OnchainSendIntentRecord, now: u64) -> bool {
        match &send.state {
            OnchainSendIntentState::Attempting { started_at, .. } => {
                started_at.saturating_add(SEND_ATTEMPT_REVIEW_SECS) <= now
            }
            OnchainSendIntentState::Broadcast { .. } => true,
            OnchainSendIntentState::NeedsReview { .. }
            | OnchainSendIntentState::Confirmed { .. } => false,
        }
    }

    async fn reconcile_onchain_send(
        &self,
        quote_id_str: &str,
    ) -> Result<Option<OnchainSendIntentRecord>, cdk_common::payment::Error> {
        let Some(send) = self.state_store.get_send(quote_id_str)? else {
            return Ok(None);
        };

        let now = Self::unix_now();
        if !Self::onchain_send_needs_reconciliation(&send, now) {
            return Ok(Some(send));
        }

        if let Err(e) = self.wallet.sync_pending_offboards().await {
            debug!("Failed to sync pending offboards: {}", e);
        }

        self.reconcile_onchain_send_record(quote_id_str, send, now)
            .await
            .map(Some)
    }

    async fn reconcile_onchain_sends(&self) -> Result<(), cdk_common::payment::Error> {
        let Ok(_send_guard) = self.onchain_send_lock.try_lock() else {
            return Ok(());
        };

        let now = Self::unix_now();
        let sends = self
            .state_store
            .sends()?
            .into_iter()
            .filter(|(_, send)| Self::onchain_send_needs_reconciliation(send, now))
            .collect();
        let window = self.state_store.rotated_records(
            ONCHAIN_SEND_RECONCILE_CURSOR_KEY,
            sends,
            MAX_INTENTS_RECONCILED_PER_TICK,
        )?;
        if window.is_empty() {
            return Ok(());
        }

        if let Err(e) = self.wallet.sync_pending_offboards().await {
            debug!("Failed to sync pending offboards: {}", e);
        }

        let last_reconciled = window.last().map(|(quote_id, _)| quote_id.clone());
        for (quote_id_str, send) in window {
            if let Err(e) = self
                .reconcile_onchain_send_record(&quote_id_str, send, now)
                .await
            {
                warn!(
                    "Failed to reconcile onchain send {} during bounded scan: {}",
                    quote_id_str, e
                );
            }
        }

        if let Some(last_reconciled) = last_reconciled {
            self.state_store
                .put_scan_cursor(ONCHAIN_SEND_RECONCILE_CURSOR_KEY, &last_reconciled)?;
        }

        Ok(())
    }

    async fn reconcile_onchain_send_record(
        &self,
        quote_id_str: &str,
        send: OnchainSendIntentRecord,
        now: u64,
    ) -> Result<OnchainSendIntentRecord, cdk_common::payment::Error> {
        match &send.state {
            OnchainSendIntentState::Attempting {
                fee_sat,
                started_at,
                ..
            } if started_at.saturating_add(SEND_ATTEMPT_REVIEW_SECS) <= now => {
                let mut needs_review = send.clone();
                needs_review.state = OnchainSendIntentState::NeedsReview {
                    reason: "Interrupted during Bark send_onchain; pending offboards are not exposed by the public Bark API for automatic recovery".to_string(),
                    fee_sat: Some(*fee_sat),
                    failed_at: now,
                };
                self.state_store.put_send(quote_id_str, &needs_review)?;
                warn!(
                    "Marked onchain send quote {} as needs_review after interrupted Bark send_onchain",
                    quote_id_str
                );
                Ok(needs_review)
            }
            OnchainSendIntentState::Attempting { .. } => Ok(send),
            OnchainSendIntentState::Broadcast { txid, fee_sat, .. } => {
                let parsed_txid = match Txid::from_str(txid) {
                    Ok(parsed_txid) => parsed_txid,
                    Err(e) => {
                        warn!(
                            "Skipping onchain send with invalid stored offboard txid {}: {}",
                            txid, e
                        );
                        return Ok(send);
                    }
                };

                match self.wallet.chain().tx_status(parsed_txid).await {
                    Ok(bitcoin_ext::TxStatus::Confirmed(_)) => {
                        let mut confirmed = send.clone();
                        confirmed.state = OnchainSendIntentState::Confirmed {
                            txid: txid.clone(),
                            fee_sat: *fee_sat,
                            confirmed_at: now,
                        };
                        self.state_store.put_send(quote_id_str, &confirmed)?;
                        info!("Confirmed onchain send {} for quote {}", txid, quote_id_str);
                        Ok(confirmed)
                    }
                    Ok(bitcoin_ext::TxStatus::Mempool) | Ok(bitcoin_ext::TxStatus::NotFound) => {
                        Ok(send)
                    }
                    Err(e) => {
                        debug!("Failed to check onchain tx status for {}: {}", txid, e);
                        Ok(send)
                    }
                }
            }
            OnchainSendIntentState::NeedsReview { .. }
            | OnchainSendIntentState::Confirmed { .. } => Ok(send),
        }
    }

    fn onchain_send_response(
        &self,
        quote_id: &QuoteId,
        send: &OnchainSendIntentRecord,
        mark_completed: bool,
    ) -> Result<MakePaymentResponse, cdk_common::payment::Error> {
        let (status, payment_proof, total_spent) = match &send.state {
            OnchainSendIntentState::Confirmed { txid, fee_sat, .. } => {
                if mark_completed {
                    self.state_store
                        .mark_send_completed(&quote_id.to_string())?;
                }
                (
                    MeltQuoteState::Paid,
                    Some(txid.clone()),
                    send.amount_sat.saturating_add(*fee_sat),
                )
            }
            OnchainSendIntentState::Broadcast { txid, .. } => {
                (MeltQuoteState::Pending, Some(txid.clone()), 0)
            }
            OnchainSendIntentState::Attempting { .. }
            | OnchainSendIntentState::NeedsReview { .. } => (MeltQuoteState::Pending, None, 0),
        };

        Ok(MakePaymentResponse {
            payment_lookup_id: PaymentIdentifier::QuoteId(quote_id.clone()),
            payment_proof,
            status,
            total_spent: Amount::new(total_spent, CurrencyUnit::Sat),
        })
    }

    fn lightning_send_needs_reconciliation(send: &LightningSendIntentRecord) -> bool {
        !matches!(&send.state, LightningSendIntentState::Failed { .. })
            && (!matches!(&send.state, LightningSendIntentState::Paid { .. })
                || !send.fee_reconciled)
    }

    async fn reconcile_lightning_sends(&self) -> Result<(), cdk_common::payment::Error> {
        let Ok(_lightning_guard) = self.lightning_send_lock.try_lock() else {
            return Ok(());
        };

        let sends = self
            .state_store
            .lightning_sends()?
            .into_iter()
            .filter(|(_, send)| Self::lightning_send_needs_reconciliation(send))
            .collect();
        let window = self.state_store.rotated_records(
            LIGHTNING_SEND_RECONCILE_CURSOR_KEY,
            sends,
            MAX_INTENTS_RECONCILED_PER_TICK,
        )?;

        let last_reconciled = window.last().map(|(payment_hash, _)| payment_hash.clone());
        for (payment_hash, _) in window {
            if let Err(e) = self.reconcile_lightning_send(&payment_hash).await {
                warn!(
                    "Failed to reconcile lightning send {} during bounded scan: {}",
                    payment_hash, e
                );
            }
        }
        if let Some(last_reconciled) = last_reconciled {
            self.state_store
                .put_scan_cursor(LIGHTNING_SEND_RECONCILE_CURSOR_KEY, &last_reconciled)?;
        }
        Ok(())
    }

    async fn reconcile_lightning_send(
        &self,
        payment_hash_hex: &str,
    ) -> Result<Option<LightningSendIntentRecord>, cdk_common::payment::Error> {
        let Some(intent) = self.state_store.get_lightning_send(payment_hash_hex)? else {
            return Ok(None);
        };

        let payment_hash = Self::parse_payment_hash_hex(payment_hash_hex)?;
        match self
            .wallet
            .check_lightning_payment(PaymentHash::from(payment_hash), false)
            .await
        {
            Ok(state) => {
                let updated = self.lightning_intent_from_bark_send(intent, &state).await?;
                self.state_store
                    .put_lightning_send(payment_hash_hex, &updated)?;
                Ok(Some(updated))
            }
            Err(e) => {
                let now = Self::unix_now();
                let mut updated = intent.clone();
                if matches!(
                    intent.state,
                    LightningSendIntentState::Attempting { started_at, .. }
                        if started_at.saturating_add(SEND_ATTEMPT_REVIEW_SECS) <= now
                ) {
                    updated.state = LightningSendIntentState::NeedsReview {
                        reason: format!(
                            "Interrupted during Bark pay_lightning_invoice and no recoverable send state was found: {}",
                            e
                        ),
                        failed_at: now,
                    };
                    self.state_store
                        .put_lightning_send(payment_hash_hex, &updated)?;
                    warn!(
                        "Marked lightning send {} as needs_review after interrupted Bark payment",
                        payment_hash_hex
                    );
                    return Ok(Some(updated));
                }

                debug!(
                    "Failed to reconcile lightning send {}: {}",
                    payment_hash_hex, e
                );
                Ok(Some(intent))
            }
        }
    }

    async fn next_lightning_send_event(&self) -> Result<Option<Event>, cdk_common::payment::Error> {
        self.reconcile_lightning_sends().await?;

        let sends = self.state_store.lightning_sends()?;
        let window = self.state_store.rotated_records(
            LIGHTNING_SEND_SCAN_CURSOR_KEY,
            sends,
            MAX_SEND_INTENTS_SCANNED_PER_TICK,
        )?;
        let mut last_scanned: Option<String> = None;
        for (payment_hash, send) in window {
            last_scanned = Some(payment_hash.clone());
            if self
                .state_store
                .is_lightning_send_completed(&payment_hash)?
            {
                continue;
            }

            let quote_id = match QuoteId::from_str(&send.quote_id) {
                Ok(quote_id) => quote_id,
                Err(e) => {
                    warn!(
                        "Skipping lightning send with invalid stored quote id {}: {}",
                        send.quote_id, e
                    );
                    continue;
                }
            };

            match &send.state {
                LightningSendIntentState::Paid { .. } => {
                    self.state_store
                        .mark_lightning_send_completed(&payment_hash)?;
                    self.state_store
                        .put_scan_cursor(LIGHTNING_SEND_SCAN_CURSOR_KEY, &payment_hash)?;
                    return Ok(Some(Event::PaymentSuccessful {
                        quote_id: quote_id.clone(),
                        details: self.lightning_send_response_with_lookup(
                            &send,
                            false,
                            PaymentIdentifier::QuoteId(quote_id),
                        )?,
                    }));
                }
                LightningSendIntentState::Failed { reason, .. } => {
                    self.state_store
                        .mark_lightning_send_completed(&payment_hash)?;
                    self.state_store
                        .put_scan_cursor(LIGHTNING_SEND_SCAN_CURSOR_KEY, &payment_hash)?;
                    return Ok(Some(Event::PaymentFailed {
                        quote_id,
                        reason: reason.clone(),
                    }));
                }
                _ => {}
            }
        }

        if let Some(last) = last_scanned {
            self.state_store
                .put_scan_cursor(LIGHTNING_SEND_SCAN_CURSOR_KEY, &last)?;
        }
        Ok(None)
    }

    fn lightning_send_response_with_lookup(
        &self,
        send: &LightningSendIntentRecord,
        mark_completed: bool,
        payment_lookup_id: PaymentIdentifier,
    ) -> Result<MakePaymentResponse, cdk_common::payment::Error> {
        if mark_completed && matches!(send.state, LightningSendIntentState::Paid { .. }) {
            self.state_store
                .mark_lightning_send_completed(&send.payment_hash)?;
        }
        let (status, payment_proof, total_spent) = Self::lightning_send_response_details(send)?;

        Ok(MakePaymentResponse {
            payment_lookup_id,
            payment_proof,
            status,
            total_spent: Amount::new(total_spent, CurrencyUnit::Sat),
        })
    }

    fn lightning_send_response_details(
        send: &LightningSendIntentRecord,
    ) -> Result<(MeltQuoteState, Option<String>, u64), cdk_common::payment::Error> {
        if matches!(&send.state, LightningSendIntentState::Paid { .. }) && !send.fee_reconciled {
            return Err(cdk_common::payment::Error::Custom(format!(
                "Actual Bark fee for paid lightning payment {} has not been reconciled",
                send.payment_hash
            )));
        }

        Ok(match &send.state {
            LightningSendIntentState::Paid {
                fee_sat, preimage, ..
            } => (
                MeltQuoteState::Paid,
                Some(preimage.clone()),
                send.amount_sat.saturating_add(*fee_sat),
            ),
            LightningSendIntentState::Failed { .. } => (MeltQuoteState::Unpaid, None, 0),
            LightningSendIntentState::Attempting { .. }
            | LightningSendIntentState::Pending { .. }
            | LightningSendIntentState::NeedsReview { .. } => (MeltQuoteState::Pending, None, 0),
        })
    }

    async fn lightning_intent_from_bark_send(
        &self,
        mut intent: LightningSendIntentRecord,
        state: &bark::actions::lightning::pay::LightningSendState,
    ) -> Result<LightningSendIntentRecord, cdk_common::payment::Error> {
        use bark::actions::lightning::pay::LightningSendState;
        match state {
            LightningSendState::Paid(paid) => {
                let payment_hash = Self::parse_payment_hash_hex(&intent.payment_hash)?;
                let fee_sat = self
                    .wallet
                    .history()
                    .await
                    .map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Failed to read Bark payment history for {}: {}",
                            intent.payment_hash, e
                        ))
                    })?
                    .into_iter()
                    .find(|movement| {
                        movement.lightning_payment_hash() == Some(PaymentHash::from(payment_hash))
                    })
                    .map(|movement| movement.offchain_fee.to_sat())
                    .ok_or_else(|| {
                        cdk_common::payment::Error::Custom(format!(
                            "Bark reports lightning payment {} as paid but its actual fee is missing from payment history",
                            intent.payment_hash
                        ))
                    })?;

                // Records created before fee caps were persisted have no cap
                // to validate retroactively. The payment has already settled,
                // so report its real fee rather than leaving it unreconcilable.
                if intent.max_fee_sat.is_some() {
                    Self::enforce_lightning_fee_cap(fee_sat, intent.max_fee_sat)?;
                }
                intent.state = LightningSendIntentState::Paid {
                    fee_sat,
                    preimage: hex::encode(paid.preimage.as_ref()),
                    paid_at: Self::unix_now(),
                };
                intent.fee_reconciled = true;
            }
            LightningSendState::InProgress(send) => {
                if intent.max_fee_sat.is_some() {
                    Self::enforce_lightning_fee_cap(send.fee.to_sat(), intent.max_fee_sat)?;
                }
                intent.amount_sat = send.payment_amount.to_sat();
                intent.state = LightningSendIntentState::Pending {
                    fee_sat: send.fee.to_sat(),
                    started_at: Self::unix_now(),
                };
            }
            LightningSendState::Unknown => {
                // Bark removes a completed failed send from its payment
                // checkpoint store, so `check_lightning_payment` reports it
                // as unknown. The movement remains the authoritative durable
                // record of that terminal outcome.
                let payment_hash = Self::parse_payment_hash_hex(&intent.payment_hash)?;
                if let Some(movement) = self
                    .wallet
                    .history()
                    .await
                    .map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Failed to read Bark payment history for {}: {}",
                            intent.payment_hash, e
                        ))
                    })?
                    .into_iter()
                    .find(|movement| {
                        movement.lightning_payment_hash() == Some(PaymentHash::from(payment_hash))
                            && matches!(
                                movement.status,
                                bark::movement::MovementStatus::Failed
                                    | bark::movement::MovementStatus::Canceled
                            )
                    })
                {
                    intent.state = LightningSendIntentState::Failed {
                        reason: format!(
                            "Bark lightning payment movement finished as {}",
                            movement.status.as_str()
                        ),
                        fee_sat: Some(movement.offchain_fee.to_sat()),
                        failed_at: Self::unix_now(),
                    };
                }
            }
        }
        Ok(intent)
    }

    fn parse_payment_hash_hex(payment_hash: &str) -> Result<[u8; 32], cdk_common::payment::Error> {
        let bytes = hex::decode(payment_hash).map_err(|e| {
            cdk_common::payment::Error::Custom(format!(
                "Invalid stored payment hash {}: {}",
                payment_hash, e
            ))
        })?;
        bytes.try_into().map_err(|bytes: Vec<u8>| {
            cdk_common::payment::Error::Custom(format!(
                "Invalid stored payment hash length {}",
                bytes.len()
            ))
        })
    }

    fn enforce_lightning_fee_cap(
        fee_sat: u64,
        max_fee_sat: Option<u64>,
    ) -> Result<(), cdk_common::payment::Error> {
        let max_fee_sat = max_fee_sat.ok_or_else(|| {
            cdk_common::payment::Error::Custom(
                "Lightning payments require max_fee_amount; refusing an uncapped payment"
                    .to_string(),
            )
        })?;
        if fee_sat > max_fee_sat {
            return Err(cdk_common::payment::Error::Custom(format!(
                "Bark lightning fee {} sat exceeds max fee {} sat",
                fee_sat, max_fee_sat
            )));
        }
        Ok(())
    }

    fn parse_arkoor_request(
        method: &str,
        request: &str,
        amount: Option<&Amount<CurrencyUnit>>,
        extra_json: Option<&str>,
        quoted_amount_sat: Option<u64>,
    ) -> Result<(ark::Address, u64), cdk_common::payment::Error> {
        // The current payment-processor protocol does not carry the custom method name
        // over gRPC. Bark exposes only one custom method, so an empty method
        // is unambiguous.
        if !method.is_empty() && method != ARKOOR_PAYMENT_METHOD {
            return Err(cdk_common::payment::Error::UnsupportedPaymentOption);
        }

        let address = ark::Address::from_str(request)
            .map_err(|e| cdk_common::payment::Error::Custom(format!("Invalid Ark address: {e}")))?;

        let typed_amount_sat = amount
            .map(|amount| {
                if amount.unit() != &CurrencyUnit::Sat || amount.value() == 0 {
                    return Err(cdk_common::payment::Error::Custom(
                        "Arkoor amount must be a positive sat amount".to_string(),
                    ));
                }
                Ok(amount.value())
            })
            .transpose()?;
        let extra_amount_sat = extra_json
            .map(|extra_json| {
                let extra: serde_json::Value = serde_json::from_str(extra_json).map_err(|e| {
                    cdk_common::payment::Error::Custom(format!("Invalid arkoor extra_json: {e}"))
                })?;
                let Some(amount) = extra.get("amount_sat") else {
                    return Ok(None);
                };
                amount
                    .as_u64()
                    .filter(|amount| *amount > 0)
                    .map(Some)
                    .ok_or_else(|| {
                        cdk_common::payment::Error::Custom(
                            "Arkoor extra_json.amount_sat must be a positive integer".to_string(),
                        )
                    })
            })
            .transpose()?
            .flatten();

        let candidates = [typed_amount_sat, extra_amount_sat, quoted_amount_sat];
        let amount_sat = candidates.into_iter().flatten().next().ok_or_else(|| {
            cdk_common::payment::Error::Custom(
                "Arkoor payments require a typed amount or extra_json.amount_sat".to_string(),
            )
        })?;
        if candidates
            .into_iter()
            .flatten()
            .any(|candidate| candidate != amount_sat)
        {
            return Err(cdk_common::payment::Error::Custom(
                "Arkoor payment amount does not match the quoted amount".to_string(),
            ));
        }

        Ok((address, amount_sat))
    }

    async fn validate_arkoor_request(
        &self,
        method: &str,
        request: &str,
        amount: Option<&Amount<CurrencyUnit>>,
        extra_json: Option<&str>,
        quoted_amount_sat: Option<u64>,
    ) -> Result<(ark::Address, u64), cdk_common::payment::Error> {
        let (address, amount_sat) =
            Self::parse_arkoor_request(method, request, amount, extra_json, quoted_amount_sat)?;
        self.wallet
            .validate_arkoor_address(&address)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Invalid arkoor destination: {e}"))
            })?;
        Ok((address, amount_sat))
    }

    async fn successful_arkoor_payments(
        &self,
        address: &str,
        amount_sat: u64,
    ) -> Result<Vec<bark::movement::Movement>, cdk_common::payment::Error> {
        let mut movements = self.wallet.history().await.map_err(|e| {
            cdk_common::payment::Error::Custom(format!(
                "Failed to read Bark history while reconciling arkoor payment: {e}"
            ))
        })?;
        movements.retain(|movement| {
            movement.status == bark::movement::MovementStatus::Successful
                && movement.sent_to.iter().any(|destination| {
                    destination.amount.to_sat() == amount_sat
                        && destination.destination.value_string() == address
                })
        });
        movements.sort_by_key(|movement| movement.id);
        Ok(movements)
    }

    async fn reconcile_arkoor_send(
        &self,
        quote_id: &str,
    ) -> Result<Option<ArkoorSendIntentRecord>, cdk_common::payment::Error> {
        let Some(mut send) = self.state_store.get_arkoor_send(quote_id)? else {
            return Ok(None);
        };
        if !matches!(send.state, ArkoorSendIntentState::Attempting { .. }) {
            return Ok(Some(send));
        }

        if let Err(e) = self.wallet.sync_pending_arkoor_sends().await {
            debug!("Failed to drive pending arkoor sends during reconciliation: {e}");
        }
        let successful = self
            .successful_arkoor_payments(&send.address, send.amount_sat)
            .await?;
        if successful.len() > send.successful_payments_before {
            let movement = successful
                .last()
                .expect("successful arkoor payment count increased");
            send.state = ArkoorSendIntentState::Paid {
                payment_proof: format!("arkoor-movement-{}", movement.id),
                paid_at: Self::unix_now(),
            };
            self.state_store.put_arkoor_send(quote_id, &send)?;
        }

        Ok(Some(send))
    }

    fn arkoor_send_response(
        &self,
        send: &ArkoorSendIntentRecord,
        mark_completed: bool,
    ) -> Result<MakePaymentResponse, cdk_common::payment::Error> {
        let quote_id = QuoteId::from_str(&send.quote_id).map_err(|e| {
            cdk_common::payment::Error::Custom(format!(
                "Invalid stored arkoor quote id {}: {e}",
                send.quote_id
            ))
        })?;
        let (status, payment_proof, total_spent) = match &send.state {
            ArkoorSendIntentState::Paid { payment_proof, .. } => {
                if mark_completed {
                    self.state_store
                        .mark_arkoor_send_completed(&send.quote_id)?;
                }
                (
                    MeltQuoteState::Paid,
                    Some(payment_proof.clone()),
                    send.amount_sat,
                )
            }
            ArkoorSendIntentState::Attempting { .. }
            | ArkoorSendIntentState::NeedsReview { .. } => (MeltQuoteState::Pending, None, 0),
        };
        Ok(MakePaymentResponse {
            payment_lookup_id: PaymentIdentifier::QuoteId(quote_id),
            payment_proof,
            status,
            total_spent: Amount::new(total_spent, CurrencyUnit::Sat),
        })
    }

    async fn next_arkoor_send_event(&self) -> Result<Option<Event>, cdk_common::payment::Error> {
        let Ok(_send_guard) = self.arkoor_send_lock.try_lock() else {
            return Ok(None);
        };
        let sends = self.state_store.arkoor_sends()?;
        let window = self.state_store.rotated_records(
            ARKOOR_SEND_SCAN_CURSOR_KEY,
            sends,
            MAX_SEND_INTENTS_SCANNED_PER_TICK,
        )?;
        let mut last_scanned = None;
        for (quote_id, _) in window {
            last_scanned = Some(quote_id.clone());
            if self.state_store.is_arkoor_send_completed(&quote_id)? {
                continue;
            }
            let Some(send) = self.reconcile_arkoor_send(&quote_id).await? else {
                continue;
            };
            if matches!(send.state, ArkoorSendIntentState::Paid { .. }) {
                let quote_id = QuoteId::from_str(&quote_id).map_err(|e| {
                    cdk_common::payment::Error::Custom(format!(
                        "Invalid stored arkoor quote id: {e}"
                    ))
                })?;
                let details = self.arkoor_send_response(&send, false)?;
                self.state_store
                    .mark_arkoor_send_completed(&send.quote_id)?;
                self.state_store
                    .put_scan_cursor(ARKOOR_SEND_SCAN_CURSOR_KEY, &send.quote_id)?;
                return Ok(Some(Event::PaymentSuccessful { quote_id, details }));
            }
        }
        if let Some(last) = last_scanned {
            self.state_store
                .put_scan_cursor(ARKOOR_SEND_SCAN_CURSOR_KEY, &last)?;
        }
        Ok(None)
    }

    /// Convert bitcoin::Amount to CDK Amount (instance method)
    fn btc_amount_to_cdk(&self, amount: bitcoin::Amount) -> Amount<CurrencyUnit> {
        Amount::new(amount.to_sat(), CurrencyUnit::Sat)
    }

    async fn check_lightning_receive(
        &self,
        payment_identifier: PaymentIdentifier,
        payment_hash: PaymentHash,
        mark_reported: bool,
    ) -> Result<Vec<WaitPaymentResponse>, cdk_common::payment::Error> {
        let state = self
            .wallet
            .lightning_receive_state(payment_hash)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to check receive status: {}", e))
            })?;

        if let bark::actions::lightning::receive::LightningReceiveState::Settled(receive) = state {
            let amount = self.btc_amount_to_cdk(receive.amount);
            let payment_hash_bytes: [u8; 32] = payment_hash.into();
            let response = WaitPaymentResponse {
                payment_amount: amount,
                payment_id: hex::encode(payment_hash_bytes),
                payment_identifier,
            };
            if mark_reported {
                self.state_store
                    .mark_lightning_receive_reported(&response.payment_identifier.to_string())?;
            }
            return Ok(vec![response]);
        }

        Ok(vec![])
    }

    async fn next_lightning_receive_event(
        &self,
    ) -> Result<Option<Event>, cdk_common::payment::Error> {
        let quotes = self.state_store.lightning_receive_quotes()?;
        let window = self.state_store.rotated_records(
            LIGHTNING_RECEIVE_SCAN_CURSOR_KEY,
            quotes,
            MAX_RECEIVE_QUOTES_SCANNED_PER_TICK,
        )?;
        let mut last_scanned: Option<String> = None;
        for (quote_id_str, payment_hash_hex) in window {
            last_scanned = Some(quote_id_str.clone());
            let quote_id = match QuoteId::from_str(&quote_id_str) {
                Ok(quote_id) => quote_id,
                Err(e) => {
                    warn!(
                        "Skipping lightning receive quote with invalid stored quote id {}: {}",
                        quote_id_str, e
                    );
                    continue;
                }
            };
            let payment_identifier = PaymentIdentifier::QuoteId(quote_id);
            let request_lookup_id = payment_identifier.to_string();
            if self
                .state_store
                .is_lightning_receive_reported(&request_lookup_id)?
            {
                continue;
            }

            let payment_hash_bytes = match Self::parse_payment_hash_hex(&payment_hash_hex) {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!("Skipping lightning receive quote {}: {}", quote_id_str, e);
                    continue;
                }
            };
            let state = match self
                .wallet
                .lightning_receive_state(PaymentHash::from(payment_hash_bytes))
                .await
            {
                Ok(state) => state,
                Err(e) => {
                    debug!(
                        "Failed to check lightning receive {}: {}",
                        payment_hash_hex, e
                    );
                    continue;
                }
            };
            let bark::actions::lightning::receive::LightningReceiveState::Settled(receive) = state
            else {
                continue;
            };

            let response = WaitPaymentResponse {
                payment_identifier,
                payment_amount: self.btc_amount_to_cdk(receive.amount),
                payment_id: payment_hash_hex,
            };

            self.state_store
                .mark_lightning_receive_reported(&request_lookup_id)?;
            self.state_store
                .put_scan_cursor(LIGHTNING_RECEIVE_SCAN_CURSOR_KEY, &quote_id_str)?;
            return Ok(Some(Event::PaymentReceived(response)));
        }

        if let Some(last) = last_scanned {
            self.state_store
                .put_scan_cursor(LIGHTNING_RECEIVE_SCAN_CURSOR_KEY, &last)?;
        }
        Ok(None)
    }

    fn unix_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default()
    }
}

#[async_trait]
impl MintPayment for BarkBackend {
    type Err = cdk_common::payment::Error;

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        debug!("Getting Bark wallet settings");
        Ok(settings_response(&self.advertised_methods))
    }

    async fn create_incoming_payment_request(
        &self,
        options: IncomingPaymentOptions,
    ) -> Result<CreateIncomingPaymentResponse, Self::Err> {
        debug!("Creating incoming payment request");

        let bolt11_options = match options {
            IncomingPaymentOptions::Bolt11(opts) => Some(opts),
            IncomingPaymentOptions::Onchain(opts) => {
                let address = {
                    let mut onchain = self.onchain_wallet.write().await;
                    onchain.sync(self.wallet.chain()).await.map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Failed to sync onchain wallet: {}",
                            e
                        ))
                    })?;
                    onchain.address().await.map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Failed to create onchain address: {}",
                            e
                        ))
                    })?
                };

                let quote_id = opts.quote_id;
                let quote_id_str = quote_id.to_string();
                let address_str = address.to_string();
                self.state_store
                    .put_receive_address(&quote_id_str, &address_str)?;

                info!(
                    "Created onchain receive address {} for quote {}",
                    address_str, quote_id
                );

                return Ok(CreateIncomingPaymentResponse {
                    request_lookup_id: PaymentIdentifier::QuoteId(quote_id),
                    request: address_str,
                    expiry: None,
                    extra_json: Some(serde_json::json!({
                        "fee_policy": "bark_board_fee_deducted_from_received_amount",
                    })),
                });
            }
            _ => {
                return Err(cdk_common::payment::Error::UnsupportedPaymentOption);
            }
        };
        let bolt11_options = bolt11_options.expect("BOLT11 branch returns Some");

        // Only support sat unit
        if bolt11_options.amount.unit().to_string() != "sat" {
            return Err(cdk_common::payment::Error::UnsupportedUnit);
        }

        // Convert amount to bitcoin::Amount - use to_u64() to get raw value from Amount<()>
        let amount = bitcoin::Amount::from_sat(bolt11_options.amount.to_u64());

        // Generate BOLT11 invoice using bark wallet
        let invoice = self
            .wallet
            .bolt11_invoice(amount, bolt11_options.description, None)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to create invoice: {}", e))
            })?;

        // Extract payment hash from the invoice - bark returns lightning_invoice::Bolt11Invoice
        let payment_hash_bytes: [u8; 32] = *invoice.payment_hash().as_ref();
        let payment_hash_hex = hex::encode(payment_hash_bytes);
        let quote_id = QuoteId::new();
        self.state_store
            .put_lightning_receive_quote(&quote_id.to_string(), &payment_hash_hex)?;

        // CDK expects an absolute Unix expiry, while BOLT11 stores a creation
        // timestamp plus a relative expiry duration.
        let expiry = invoice.expires_at().map(|duration| duration.as_secs());

        // Convert invoice to string
        let invoice_str = invoice.to_string();

        info!(
            "Created BOLT11 invoice for {} sat, quote_id: {}, payment_hash: {}",
            amount.to_sat(),
            quote_id,
            payment_hash_hex
        );

        Ok(CreateIncomingPaymentResponse {
            request_lookup_id: PaymentIdentifier::QuoteId(quote_id),
            request: invoice_str,
            expiry,
            extra_json: None,
        })
    }

    async fn get_payment_quote(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<PaymentQuoteResponse, Self::Err> {
        debug!("Getting payment quote");

        // Only support sat unit
        if unit.to_string() != "sat" {
            return Err(cdk_common::payment::Error::UnsupportedUnit);
        }

        match options {
            OutgoingPaymentOptions::Bolt11(opts) => {
                let invoice = &opts.bolt11;

                if invoice.is_expired() {
                    return Err(cdk_common::payment::Error::Custom(
                        "Invoice is expired".to_string(),
                    ));
                }

                let amount_msat = invoice.amount_milli_satoshis().ok_or_else(|| {
                    cdk_common::payment::Error::Custom("Invoice has no amount".to_string())
                })?;
                let amount_sat = amount_msat / 1000;

                let fee_sats = self
                    .wallet
                    .estimate_lightning_send_fee(bitcoin::Amount::from_sat(amount_sat))
                    .await
                    .map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Failed to estimate Bark lightning fee: {}",
                            e
                        ))
                    })?
                    .fee
                    .to_sat();
                debug!("Payment quote: {} sat + {} sat fee", amount_sat, fee_sats);

                Ok(PaymentQuoteResponse {
                    request_lookup_id: Some(PaymentIdentifier::QuoteId(opts.quote_id.clone())),
                    amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                    fee: Amount::new(fee_sats, CurrencyUnit::Sat),
                    state: MeltQuoteState::Unpaid,
                    extra_json: None,
                    estimated_blocks: None,
                    fee_options: None,
                })
            }
            OutgoingPaymentOptions::Onchain(opts) => {
                let address = self.parse_bitcoin_address(&opts.address)?;
                let amount_sat = opts.amount.to_u64();
                let amount = bitcoin::Amount::from_sat(amount_sat);
                let estimate = self
                    .wallet
                    .estimate_send_onchain(&address, amount)
                    .await
                    .map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Failed to estimate onchain payment: {}",
                            e
                        ))
                    })?;
                let fee_sat = estimate.fee.to_sat();
                let fee_options = vec![MeltQuoteOnchainFeeOption {
                    fee_index: ONCHAIN_FEE_INDEX,
                    fee_reserve: Amount::from(fee_sat),
                    estimated_blocks: ONCHAIN_ESTIMATED_BLOCKS,
                }];

                return Ok(PaymentQuoteResponse {
                    request_lookup_id: Some(PaymentIdentifier::QuoteId(opts.quote_id.clone())),
                    amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                    fee: Amount::new(fee_sat, CurrencyUnit::Sat),
                    state: MeltQuoteState::Unpaid,
                    extra_json: None,
                    estimated_blocks: Some(ONCHAIN_ESTIMATED_BLOCKS),
                    fee_options: Some(fee_options),
                });
            }
            OutgoingPaymentOptions::Custom(opts) => {
                let (_, amount_sat) = self
                    .validate_arkoor_request(
                        &opts.method,
                        &opts.request,
                        opts.amount.as_ref(),
                        opts.extra_json.as_deref(),
                        None,
                    )
                    .await?;
                self.state_store.put_arkoor_quote(
                    &opts.quote_id.to_string(),
                    &ArkoorQuoteRecord {
                        request: opts.request.clone(),
                        amount_sat,
                    },
                )?;
                Ok(PaymentQuoteResponse {
                    request_lookup_id: Some(PaymentIdentifier::QuoteId(opts.quote_id.clone())),
                    amount: Amount::new(amount_sat, CurrencyUnit::Sat),
                    fee: Amount::new(0, CurrencyUnit::Sat),
                    state: MeltQuoteState::Unpaid,
                    extra_json: Some(serde_json::json!({"routing": ARKOOR_PAYMENT_METHOD})),
                    estimated_blocks: None,
                    fee_options: None,
                })
            }
            _ => Err(cdk_common::payment::Error::UnsupportedPaymentOption),
        }
    }

    async fn make_payment(
        &self,
        unit: &CurrencyUnit,
        options: OutgoingPaymentOptions,
    ) -> Result<MakePaymentResponse, Self::Err> {
        debug!("Making payment");

        // Only support sat unit
        if unit.to_string() != "sat" {
            return Err(cdk_common::payment::Error::UnsupportedUnit);
        }

        let bolt11_options = match options {
            OutgoingPaymentOptions::Bolt11(opts) => opts,
            OutgoingPaymentOptions::Custom(opts) => {
                let _send_guard = self.arkoor_send_lock.lock().await;
                let quote_id = opts.quote_id.to_string();
                if let Some(existing) = self.reconcile_arkoor_send(&quote_id).await? {
                    return self.arkoor_send_response(&existing, false);
                }

                let quoted = self.state_store.get_arkoor_quote(&quote_id)?;
                if quoted
                    .as_ref()
                    .is_some_and(|quote| quote.request != opts.request)
                {
                    return Err(cdk_common::payment::Error::Custom(
                        "Arkoor request does not match the quoted request".to_string(),
                    ));
                }

                let (address, amount_sat) = self
                    .validate_arkoor_request(
                        &opts.method,
                        &opts.request,
                        opts.amount.as_ref(),
                        opts.extra_json.as_deref(),
                        quoted.as_ref().map(|quote| quote.amount_sat),
                    )
                    .await?;
                let successful_payments_before = self
                    .successful_arkoor_payments(&opts.request, amount_sat)
                    .await?
                    .len();
                let mut send = ArkoorSendIntentRecord {
                    quote_id: quote_id.clone(),
                    address: opts.request.clone(),
                    amount_sat,
                    successful_payments_before,
                    state: ArkoorSendIntentState::Attempting {
                        attempt_id: uuid::Uuid::new_v4().to_string(),
                        started_at: Self::unix_now(),
                    },
                };
                self.state_store.put_arkoor_send(&quote_id, &send)?;

                match self
                    .wallet
                    .send_arkoor_payment(&address, bitcoin::Amount::from_sat(amount_sat))
                    .await
                {
                    Ok(()) => {
                        let successful = self
                            .successful_arkoor_payments(&opts.request, amount_sat)
                            .await?;
                        let payment_proof = successful
                            .last()
                            .map(|movement| format!("arkoor-movement-{}", movement.id))
                            .unwrap_or_else(|| format!("arkoor-quote-{quote_id}"));
                        send.state = ArkoorSendIntentState::Paid {
                            payment_proof,
                            paid_at: Self::unix_now(),
                        };
                        self.state_store.put_arkoor_send(&quote_id, &send)?;
                        return self.arkoor_send_response(&send, false);
                    }
                    Err(e) => {
                        let reason = e.to_string();
                        if let Some(reconciled) = self.reconcile_arkoor_send(&quote_id).await? {
                            if matches!(reconciled.state, ArkoorSendIntentState::Paid { .. }) {
                                return self.arkoor_send_response(&reconciled, false);
                            }
                            send = reconciled;
                        }
                        send.state = ArkoorSendIntentState::NeedsReview {
                            reason: format!(
                                "Bark arkoor send returned an error after the attempt started: {reason}"
                            ),
                            failed_at: Self::unix_now(),
                        };
                        self.state_store.put_arkoor_send(&quote_id, &send)?;
                        return Err(cdk_common::payment::Error::Custom(format!(
                            "Failed to send arkoor payment: {reason}"
                        )));
                    }
                }
            }
            OutgoingPaymentOptions::Onchain(opts) => {
                if !matches!(opts.fee_index, None | Some(ONCHAIN_FEE_INDEX)) {
                    return Err(cdk_common::payment::Error::Custom(format!(
                        "Unsupported onchain fee_index {:?}",
                        opts.fee_index
                    )));
                }

                let _send_guard = self.onchain_send_lock.lock().await;
                let quote_id_str = opts.quote_id.to_string();
                match self.reconcile_onchain_send(&quote_id_str).await {
                    Ok(Some(existing_send)) => {
                        return self.onchain_send_response(&opts.quote_id, &existing_send, false);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(
                            "Failed to reconcile existing onchain send for quote {}: {}",
                            quote_id_str, e
                        );
                        match self.state_store.get_send(&quote_id_str) {
                            Ok(Some(existing_send)) => {
                                return self.onchain_send_response(
                                    &opts.quote_id,
                                    &existing_send,
                                    false,
                                );
                            }
                            Ok(None) => {}
                            Err(e) => {
                                warn!(
                                    "Failed to look up existing onchain send for quote {}: {}",
                                    quote_id_str, e
                                );
                            }
                        }
                    }
                }

                let address = self.parse_bitcoin_address(&opts.address)?;
                let address_str = address.to_string();
                let amount_sat = opts.amount.to_u64();
                let amount = bitcoin::Amount::from_sat(amount_sat);
                let estimate = self
                    .wallet
                    .estimate_send_onchain(&address, amount)
                    .await
                    .map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Failed to estimate onchain payment: {}",
                            e
                        ))
                    })?;

                if let Some(max_fee) = opts.max_fee_amount.as_ref() {
                    let max_fee_sat = max_fee.clone().to_u64();
                    if estimate.fee.to_sat() > max_fee_sat {
                        return Err(cdk_common::payment::Error::Custom(format!(
                            "Estimated onchain fee {} sat exceeds max fee {} sat",
                            estimate.fee.to_sat(),
                            max_fee_sat
                        )));
                    }
                }

                let fee_sat = estimate.fee.to_sat();
                let mut send_intent = OnchainSendIntentRecord {
                    quote_id: quote_id_str.clone(),
                    address: address_str,
                    amount_sat,
                    state: OnchainSendIntentState::Attempting {
                        attempt: 1,
                        attempt_id: uuid::Uuid::new_v4().to_string(),
                        fee_sat,
                        started_at: Self::unix_now(),
                    },
                };
                self.state_store.put_send(&quote_id_str, &send_intent)?;

                let txid = match self.wallet.send_onchain(address, amount).await {
                    Ok(txid) => txid,
                    Err(e) => {
                        let reason = e.to_string();
                        send_intent.state = OnchainSendIntentState::NeedsReview {
                            reason: format!(
                                "Bark send_onchain returned an error after the offboard attempt was started: {}",
                                reason
                            ),
                            fee_sat: Some(fee_sat),
                            failed_at: Self::unix_now(),
                        };
                        self.state_store.put_send(&quote_id_str, &send_intent)?;
                        return Err(cdk_common::payment::Error::Custom(format!(
                            "Failed to send onchain payment: {}",
                            reason
                        )));
                    }
                };

                send_intent.state = OnchainSendIntentState::Broadcast {
                    txid: txid.to_string(),
                    fee_sat,
                    broadcast_at: Self::unix_now(),
                };
                self.state_store.put_send(&quote_id_str, &send_intent)?;

                info!(
                    "Broadcasted onchain payment {} for quote {}",
                    txid, opts.quote_id
                );

                return Ok(MakePaymentResponse {
                    payment_lookup_id: PaymentIdentifier::QuoteId(opts.quote_id),
                    payment_proof: Some(txid.to_string()),
                    status: MeltQuoteState::Pending,
                    total_spent: Amount::new(0, CurrencyUnit::Sat),
                });
            }
            _ => {
                return Err(cdk_common::payment::Error::UnsupportedPaymentOption);
            }
        };

        // bolt11_options.bolt11 is already a parsed invoice
        let invoice = &bolt11_options.bolt11;

        if invoice.is_expired() {
            return Err(cdk_common::payment::Error::Custom(
                "Invoice is expired".to_string(),
            ));
        }

        // Extract payment hash
        let payment_hash: [u8; 32] = *invoice.payment_hash().as_ref();
        let payment_hash_hex = hex::encode(payment_hash);
        let payment_lookup_id = PaymentIdentifier::QuoteId(bolt11_options.quote_id.clone());
        let quote_id_str = bolt11_options.quote_id.to_string();

        // Get the amount from the invoice
        let amount_msat = invoice.amount_milli_satoshis().ok_or_else(|| {
            cdk_common::payment::Error::Custom("Invoice has no amount".to_string())
        })?;
        let amount_sat = amount_msat / 1000;
        let _lightning_send_guard = self.lightning_send_lock.lock().await;
        match self.state_store.lightning_send_for_quote(&quote_id_str) {
            Ok(Some((existing_payment_hash, _))) => {
                match self.reconcile_lightning_send(&existing_payment_hash).await {
                    Ok(Some(existing_send)) => {
                        return self.lightning_send_response_with_lookup(
                            &existing_send,
                            false,
                            payment_lookup_id.clone(),
                        );
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(
                            "Failed to reconcile existing lightning send for quote {}: {}",
                            quote_id_str, e
                        );
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "Failed to look up existing lightning send for quote {}: {}",
                    quote_id_str, e
                );
            }
        }
        match self.reconcile_lightning_send(&payment_hash_hex).await {
            Ok(Some(existing_send)) => {
                return self.lightning_send_response_with_lookup(
                    &existing_send,
                    false,
                    payment_lookup_id.clone(),
                );
            }
            Ok(None) => {}
            Err(e) => {
                warn!(
                    "Failed to reconcile lightning send {} before payment: {}",
                    payment_hash_hex, e
                );
            }
        }

        let invoice_str = invoice.to_string();
        // Bark snapshots the Ark server's fee schedule when it connects. Its
        // estimator and payment builder therefore calculate from the same
        // schedule; holding the send lock also keeps the selected wallet inputs
        // stable between this check and starting the payment.
        let max_fee_sat = bolt11_options
            .max_fee_amount
            .as_ref()
            .map(|max_fee| max_fee.clone().to_u64());
        let preflight_fee_sat = self
            .wallet
            .estimate_lightning_send_fee(bitcoin::Amount::from_sat(amount_sat))
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!(
                    "Failed to estimate Bark lightning fee before payment: {}",
                    e
                ))
            })?
            .fee
            .to_sat();
        Self::enforce_lightning_fee_cap(preflight_fee_sat, max_fee_sat)?;

        let mut send_intent = LightningSendIntentRecord {
            quote_id: bolt11_options.quote_id.to_string(),
            payment_hash: payment_hash_hex.clone(),
            invoice: invoice_str.clone(),
            amount_sat,
            preflight_fee_sat,
            max_fee_sat,
            fee_reconciled: false,
            state: LightningSendIntentState::Attempting {
                attempt: 1,
                attempt_id: uuid::Uuid::new_v4().to_string(),
                started_at: Self::unix_now(),
            },
        };
        self.state_store
            .put_lightning_send(&payment_hash_hex, &send_intent)?;

        if let Err(e) = self
            .wallet
            .pay_lightning_invoice(invoice_str.as_str(), None, false)
            .await
        {
            let reason = e.to_string();
            match self
                .wallet
                .check_lightning_payment(PaymentHash::from(payment_hash), false)
                .await
            {
                Ok(state)
                    if !matches!(
                        state,
                        bark::actions::lightning::pay::LightningSendState::Unknown
                    ) =>
                {
                    let recovered = self
                        .lightning_intent_from_bark_send(send_intent, &state)
                        .await?;
                    self.state_store
                        .put_lightning_send(&payment_hash_hex, &recovered)?;
                }
                _ => {
                    send_intent.state = LightningSendIntentState::NeedsReview {
                        reason: format!(
                            "Bark pay_lightning_invoice returned an error after the payment attempt was started: {}",
                            reason
                        ),
                        failed_at: Self::unix_now(),
                    };
                    self.state_store
                        .put_lightning_send(&payment_hash_hex, &send_intent)?;
                }
            }
            return Err(cdk_common::payment::Error::Custom(format!(
                "Failed to pay invoice: {}",
                reason
            )));
        }

        let state = self
            .wallet
            .check_lightning_payment(PaymentHash::from(payment_hash), false)
            .await
            .unwrap_or(bark::actions::lightning::pay::LightningSendState::Unknown);
        let updated_send = self
            .lightning_intent_from_bark_send(send_intent, &state)
            .await?;
        self.state_store
            .put_lightning_send(&payment_hash_hex, &updated_send)?;

        info!(
            "Started lightning payment for {} sat, payment_hash: {}",
            amount_sat, payment_hash_hex
        );

        self.lightning_send_response_with_lookup(&updated_send, false, payment_lookup_id)
    }

    async fn wait_payment_event(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Event> + Send>>, Self::Err> {
        debug!("Starting payment event stream");
        self.wait_invoice_active.store(true, Ordering::SeqCst);

        let backend = self.clone();
        let wallet = self.wallet.clone();
        let active = self.wait_invoice_active.clone();
        let poll_interval = self.event_poll_interval;

        // Create a stream that polls for incoming payments
        let stream = stream::unfold(
            (backend, wallet, active, poll_interval, 0usize),
            |(backend, wallet, active, poll_interval, tick)| async move {
                // Check if we should stop
                if !active.load(Ordering::SeqCst) {
                    return None;
                }

                // Wait for the polling interval
                tokio::time::sleep(poll_interval).await;

                // Try to claim all lightning receives, waiting for an ongoing
                // wallet-side claim to finish instead of dropping this tick.
                if let Err(e) = wallet.try_claim_all_lightning_receives(true).await {
                    warn!("Failed to claim lightning receives: {}", e);
                }

                // Rotate the starting event kind every tick so a busy kind
                // cannot starve the others.
                let mut event = None;
                for kind in 0..5 {
                    let result = match (tick + kind) % 5 {
                        0 => backend.next_lightning_receive_event().await,
                        1 => backend.next_onchain_receive_event().await,
                        2 => backend.next_lightning_send_event().await,
                        3 => backend.next_onchain_send_event().await,
                        _ => backend.next_arkoor_send_event().await,
                    };
                    match result {
                        Ok(Some(found)) => {
                            event = Some(found);
                            break;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!("Failed to poll payment events: {}", e);
                        }
                    }
                }

                let next_tick = tick.wrapping_add(1);
                Some((event, (backend, wallet, active, poll_interval, next_tick)))
            },
        )
        .filter_map(|event| async move { event });

        Ok(Box::pin(stream))
    }

    async fn check_incoming_payment_status(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<Vec<WaitPaymentResponse>, Self::Err> {
        debug!("Checking incoming payment status");

        if let PaymentIdentifier::QuoteId(quote_id) = payment_identifier {
            if let Some(payment_hash) = self
                .state_store
                .get_lightning_receive_hash(&quote_id.to_string())?
            {
                let payment_hash = Self::parse_payment_hash_hex(&payment_hash)?;
                return self
                    .check_lightning_receive(
                        payment_identifier.clone(),
                        PaymentHash::from(payment_hash),
                        true,
                    )
                    .await;
            }
            return self.check_onchain_receive(quote_id, true).await;
        }

        // Extract payment hash from identifier
        let payment_hash = match payment_identifier {
            PaymentIdentifier::PaymentHash(hash) => PaymentHash::from(*hash),
            _ => {
                return Err(cdk_common::payment::Error::Custom(
                    "Unsupported payment identifier type".to_string(),
                ));
            }
        };

        self.check_lightning_receive(payment_identifier.clone(), payment_hash, true)
            .await
    }

    async fn check_outgoing_payment(
        &self,
        payment_identifier: &PaymentIdentifier,
    ) -> Result<MakePaymentResponse, Self::Err> {
        debug!("Checking outgoing payment");

        if let PaymentIdentifier::QuoteId(quote_id) = payment_identifier {
            if let Some(response) = self.check_onchain_send(quote_id, true).await? {
                return Ok(response);
            }

            let quote_id_str = quote_id.to_string();
            if self.state_store.get_arkoor_send(&quote_id_str)?.is_some() {
                let _send_guard = self.arkoor_send_lock.lock().await;
                if let Some(send) = self.reconcile_arkoor_send(&quote_id_str).await? {
                    return self.arkoor_send_response(&send, true);
                }
            }

            match self.state_store.lightning_send_for_quote(&quote_id_str) {
                Ok(Some((payment_hash, _))) => {
                    match self.reconcile_lightning_send(&payment_hash).await {
                        Ok(Some(send)) => {
                            return self.lightning_send_response_with_lookup(
                                &send,
                                true,
                                PaymentIdentifier::QuoteId(quote_id.clone()),
                            );
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!(
                                "Failed to reconcile lightning send for quote {}: {}",
                                quote_id_str, e
                            );
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        "Failed to look up lightning send for quote {}: {}",
                        quote_id_str, e
                    );
                }
            }

            return Err(cdk_common::payment::Error::Custom(format!(
                "No outgoing payment found for quote {}",
                quote_id
            )));
        }

        Err(cdk_common::payment::Error::Custom(
            "Outgoing payment status must be checked by quote id".to_string(),
        ))
    }

    fn is_payment_event_stream_active(&self) -> bool {
        self.wait_invoice_active.load(Ordering::SeqCst)
    }

    fn cancel_payment_event_stream(&self) {
        self.wait_invoice_active.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::{settings_response, AdvertisedMethod, ARKOOR_PAYMENT_METHOD};

    #[test]
    fn every_method_is_advertised_by_default() {
        let settings = settings_response(&AdvertisedMethod::ALL);
        assert!(settings.bolt11.is_some());
        assert!(settings.onchain.is_some());
        assert!(settings.custom.contains_key(ARKOOR_PAYMENT_METHOD));
        // bolt12 has never been served by this backend.
        assert!(settings.bolt12.is_none());
    }

    #[test]
    fn onchain_only_hides_lightning_and_arkoor() {
        // The shape that lets a Core Lightning node keep bolt11: bark must not
        // claim a rail it was not configured for, or the mint rejects the
        // duplicate (unit, method) pair and refuses to start.
        let settings = settings_response(&[AdvertisedMethod::Onchain]);
        assert!(settings.onchain.is_some());
        assert!(settings.bolt11.is_none());
        assert!(settings.custom.is_empty());
    }

    #[test]
    fn bolt11_only_hides_onchain_and_arkoor() {
        let settings = settings_response(&[AdvertisedMethod::Bolt11]);
        assert!(settings.bolt11.is_some());
        assert!(settings.onchain.is_none());
        assert!(settings.custom.is_empty());
    }

    #[test]
    fn arkoor_only_advertises_just_the_custom_method() {
        let settings = settings_response(&[AdvertisedMethod::Arkoor]);
        assert!(settings.bolt11.is_none());
        assert!(settings.onchain.is_none());
        assert_eq!(settings.custom.len(), 1);
        assert!(settings.custom.contains_key(ARKOOR_PAYMENT_METHOD));
    }

    #[test]
    fn advertising_nothing_yields_no_registrable_method() {
        // Not reachable through configuration -- an empty list means "all" --
        // but the builder itself must stay honest.
        let settings = settings_response(&[]);
        assert!(settings.bolt11.is_none());
        assert!(settings.onchain.is_none());
        assert!(settings.custom.is_empty());
    }

    /// The rails a `SettingsResponse` actually claims, derived from the response
    /// rather than restated, so this tracks the real filtering.
    fn advertised_rails(methods: &[AdvertisedMethod]) -> std::collections::BTreeSet<String> {
        let settings = settings_response(methods);
        let mut rails = std::collections::BTreeSet::new();
        if settings.bolt11.is_some() {
            rails.insert("bolt11".to_string());
        }
        if settings.bolt12.is_some() {
            rails.insert("bolt12".to_string());
        }
        if settings.onchain.is_some() {
            rails.insert("onchain".to_string());
        }
        rails.extend(settings.custom.keys().cloned());
        rails
    }

    fn rails(names: &[&str]) -> std::collections::BTreeSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn deployment_matrix_leaves_no_rail_contested() {
        // A mint keys its backends by (unit, method) and refuses a duplicate
        // pair, so a deployment is valid exactly when the backends' advertised
        // rails are disjoint. Companion rails below are the defaults those
        // backends advertise: cdk-cln and cdk-payment-processor-ldk-server both
        // claim bolt11 and bolt12, and neither serves on-chain.
        let cases: &[(&str, &[AdvertisedMethod], &[&str], &[&str])] = &[
            (
                "bark alone",
                &AdvertisedMethod::ALL,
                &[],
                &["bolt11", "onchain", "arkoor"],
            ),
            (
                "bark on-chain + Core Lightning",
                &[AdvertisedMethod::Onchain, AdvertisedMethod::Arkoor],
                &["bolt11", "bolt12"],
                &["bolt11", "bolt12", "onchain", "arkoor"],
            ),
            (
                "bark on-chain + LDK Server",
                &[AdvertisedMethod::Onchain, AdvertisedMethod::Arkoor],
                &["bolt11", "bolt12"],
                &["bolt11", "bolt12", "onchain", "arkoor"],
            ),
            (
                "bark takes Lightning, companion keeps bolt12 only",
                &[
                    AdvertisedMethod::Bolt11,
                    AdvertisedMethod::Onchain,
                    AdvertisedMethod::Arkoor,
                ],
                &["bolt12"],
                &["bolt11", "bolt12", "onchain", "arkoor"],
            ),
            (
                "bark on-chain + CLN on bolt11 + LDK Server on bolt12",
                &[AdvertisedMethod::Onchain, AdvertisedMethod::Arkoor],
                &["bolt11", "bolt12"],
                &["bolt11", "bolt12", "onchain", "arkoor"],
            ),
        ];

        for (name, bark, companions, expected) in cases {
            let bark_rails = advertised_rails(bark);
            let companion_rails = rails(companions);

            let contested: Vec<_> = bark_rails.intersection(&companion_rails).collect();
            assert!(
                contested.is_empty(),
                "{name}: bark and its companion both claim {contested:?}, \
                 which the mint would reject as a duplicate (unit, method) pair"
            );

            let covered: std::collections::BTreeSet<String> =
                bark_rails.union(&companion_rails).cloned().collect();
            assert_eq!(covered, rails(expected), "{name}: rails covered");
        }
    }

    #[test]
    fn the_default_configuration_contests_lightning_with_any_lightning_backend() {
        // Why the setting has to exist. Left at its default, bark claims bolt11,
        // so pairing it with CLN or LDK Server is not a valid deployment at all
        // -- the second registration hits the same key and the mint refuses it.
        let bark_rails = advertised_rails(&AdvertisedMethod::ALL);
        for companion in ["Core Lightning", "LDK Server"] {
            let companion_rails = rails(&["bolt11", "bolt12"]);
            assert!(
                bark_rails.contains("bolt11") && companion_rails.contains("bolt11"),
                "{companion}: expected the default to contest bolt11"
            );
        }
    }

    #[test]
    fn advertised_settings_keep_their_values() {
        // Restricting the set must not quietly change the terms of a rail that
        // is still advertised.
        let all = settings_response(&AdvertisedMethod::ALL);
        let onchain_only = settings_response(&[AdvertisedMethod::Onchain]);
        assert_eq!(all.unit, onchain_only.unit);
        assert_eq!(
            all.onchain.map(|s| (
                s.confirmations,
                s.min_receive_amount_sat,
                s.min_send_amount_sat
            )),
            onchain_only.onchain.map(|s| (
                s.confirmations,
                s.min_receive_amount_sat,
                s.min_send_amount_sat
            )),
        );
    }

    use super::*;

    static NEXT_TEST_STORE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn test_store_path() -> PathBuf {
        let unique = NEXT_TEST_STORE_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "cdk-bark-state-{}-{unique}.redb",
            std::process::id()
        ))
    }

    fn test_store() -> BarkStateStore {
        BarkStateStore::open(test_store_path()).expect("create state store")
    }

    fn finalized_receive(
        quote_id: &str,
        outpoint: &str,
        amount_sat: u64,
    ) -> OnchainReceiveIntentRecord {
        OnchainReceiveIntentRecord {
            quote_id: quote_id.to_string(),
            deposit_outpoint: outpoint.to_string(),
            gross_sat: amount_sat,
            state: OnchainReceiveIntentState::Finalized {
                board_txid: "txid".to_string(),
                board_vtxo_ids: vec![],
                fee_sat: 0,
                amount_sat,
                finalized_at: 0,
            },
        }
    }

    fn onchain_send(quote_id: &str) -> OnchainSendIntentRecord {
        OnchainSendIntentRecord {
            quote_id: quote_id.to_string(),
            address: "bcrt1qexample".to_string(),
            amount_sat: 1_000,
            state: OnchainSendIntentState::NeedsReview {
                reason: "test".to_string(),
                fee_sat: None,
                failed_at: 0,
            },
        }
    }

    fn lightning_send(payment_hash: &str, quote_id: &str) -> LightningSendIntentRecord {
        LightningSendIntentRecord {
            quote_id: quote_id.to_string(),
            payment_hash: payment_hash.to_string(),
            invoice: "invoice".to_string(),
            amount_sat: 1_000,
            preflight_fee_sat: 1,
            max_fee_sat: Some(1),
            fee_reconciled: false,
            state: LightningSendIntentState::NeedsReview {
                reason: "test".to_string(),
                failed_at: 0,
            },
        }
    }

    fn arkoor_send(quote_id: &str) -> ArkoorSendIntentRecord {
        ArkoorSendIntentRecord {
            quote_id: quote_id.to_string(),
            address: "tark1ptest".to_string(),
            amount_sat: 1_000,
            successful_payments_before: 0,
            state: ArkoorSendIntentState::Paid {
                payment_proof: "arkoor-movement-1".to_string(),
                paid_at: 1,
            },
        }
    }

    #[test]
    fn supported_networks_are_explicit_and_unknown_values_fail() {
        assert_eq!(
            BarkBackend::parse_network("mainnet").unwrap(),
            bitcoin::Network::Bitcoin
        );
        assert_eq!(
            BarkBackend::parse_network("TESTNET").unwrap(),
            bitcoin::Network::Testnet
        );
        assert_eq!(
            BarkBackend::parse_network("signet").unwrap(),
            bitcoin::Network::Signet
        );
        assert_eq!(
            BarkBackend::parse_network("Regtest").unwrap(),
            bitcoin::Network::Regtest
        );
        let error = BarkBackend::parse_network("typo-net").unwrap_err();
        assert!(error.to_string().contains("Unsupported Bark network"));
    }

    #[test]
    fn arkoor_custom_request_contract_is_strict() {
        let address = "ark1pndckx4ezqqp4cn00sj5cswh7vrhh9vm647qr3ht5a57s4vdp7vrpptxv66x3ehfzqyp4cn00sj5cswh7vrhh9vm647qr3ht5a57s4vdp7vrpptxv66x3ehgjdr0q7";
        let (_, amount_sat) = BarkBackend::parse_arkoor_request(
            ARKOOR_PAYMENT_METHOD,
            address,
            None,
            Some(r#"{"amount_sat":1234}"#),
            None,
        )
        .expect("valid arkoor request");
        assert_eq!(amount_sat, 1_234);

        let typed_amount = Amount::new(2_345, CurrencyUnit::Sat);
        assert_eq!(
            BarkBackend::parse_arkoor_request(
                ARKOOR_PAYMENT_METHOD,
                address,
                Some(&typed_amount),
                None,
                None,
            )
            .expect("valid typed arkoor amount")
            .1,
            2_345
        );
        assert_eq!(
            BarkBackend::parse_arkoor_request(
                ARKOOR_PAYMENT_METHOD,
                address,
                Some(&typed_amount),
                Some(r#"{"routing":"arkoor"}"#),
                None,
            )
            .expect("typed amount with quote response metadata")
            .1,
            2_345
        );
        assert_eq!(
            BarkBackend::parse_arkoor_request(
                ARKOOR_PAYMENT_METHOD,
                address,
                None,
                None,
                Some(3_456),
            )
            .expect("persisted quote amount")
            .1,
            3_456
        );
        assert!(BarkBackend::parse_arkoor_request(
            ARKOOR_PAYMENT_METHOD,
            address,
            Some(&typed_amount),
            Some(r#"{"amount_sat":2346}"#),
            None,
        )
        .is_err());

        // The payment-processor protocol drops the custom method name on the
        // wire, so Bark intentionally accepts the unambiguous empty method.
        assert!(BarkBackend::parse_arkoor_request(
            "",
            address,
            None,
            Some(r#"{"amount_sat":1}"#),
            None,
        )
        .is_ok());
        assert!(BarkBackend::parse_arkoor_request(
            "unknown",
            address,
            None,
            Some(r#"{"amount_sat":1}"#),
            None,
        )
        .is_err());

        for extra in [
            None,
            Some("not-json"),
            Some(r#"{"amount_sat":0}"#),
            Some(r#"{"amount_sat":-1}"#),
            Some(r#"{"amount_sat":"1"}"#),
        ] {
            assert!(BarkBackend::parse_arkoor_request(
                ARKOOR_PAYMENT_METHOD,
                address,
                None,
                extra,
                None,
            )
            .is_err());
        }
        assert!(BarkBackend::parse_arkoor_request(
            ARKOOR_PAYMENT_METHOD,
            address,
            None,
            Some(r#"{}"#),
            None,
        )
        .is_err());
        assert!(BarkBackend::parse_arkoor_request(
            ARKOOR_PAYMENT_METHOD,
            "not-an-ark-address",
            None,
            Some(r#"{"amount_sat":1}"#),
            None,
        )
        .is_err());
    }

    #[test]
    fn all_payment_state_survives_store_reopen() {
        let path = test_store_path();
        let receive_quote = QuoteId::new().to_string();
        let send_quote = QuoteId::new().to_string();
        let lightning_quote = QuoteId::new().to_string();
        let arkoor_quote = QuoteId::new().to_string();
        let outpoint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0";
        let payment_hash = "ab".repeat(32);

        {
            let store = BarkStateStore::open(path.clone()).expect("create state store");
            store
                .put_receive_address(&receive_quote, "bcrt1qexample")
                .unwrap();
            store
                .put_receive_intent(&finalized_receive(&receive_quote, outpoint, 1_000))
                .unwrap();
            store.mark_onchain_receive_reported(outpoint).unwrap();
            store
                .put_lightning_receive_quote(&receive_quote, &payment_hash)
                .unwrap();
            store
                .mark_lightning_receive_reported(&receive_quote)
                .unwrap();
            store
                .put_send(&send_quote, &onchain_send(&send_quote))
                .unwrap();
            store.mark_send_completed(&send_quote).unwrap();
            store
                .put_lightning_send(
                    &payment_hash,
                    &lightning_send(&payment_hash, &lightning_quote),
                )
                .unwrap();
            store.mark_lightning_send_completed(&payment_hash).unwrap();
            store
                .put_arkoor_send(&arkoor_quote, &arkoor_send(&arkoor_quote))
                .unwrap();
            store
                .put_arkoor_quote(
                    &arkoor_quote,
                    &ArkoorQuoteRecord {
                        request: "ark-address".to_string(),
                        amount_sat: 1_000,
                    },
                )
                .unwrap();
            store.mark_arkoor_send_completed(&arkoor_quote).unwrap();
            store.put_scan_cursor("test-scan", "last-key").unwrap();
        }

        let reopened = BarkStateStore::open(path.clone()).expect("reopen state store");
        assert_eq!(
            reopened.receive_addresses().unwrap().get(&receive_quote),
            Some(&"bcrt1qexample".to_string())
        );
        assert!(reopened.get_receive_intent(outpoint).unwrap().is_some());
        assert!(reopened.is_receive_reported(outpoint).unwrap());
        assert_eq!(
            reopened.get_lightning_receive_hash(&receive_quote).unwrap(),
            Some(payment_hash.clone())
        );
        assert!(reopened
            .is_lightning_receive_reported(&receive_quote)
            .unwrap());
        assert!(reopened.get_send(&send_quote).unwrap().is_some());
        assert!(reopened.is_send_completed(&send_quote).unwrap());
        assert!(reopened
            .get_lightning_send(&payment_hash)
            .unwrap()
            .is_some());
        assert!(reopened.is_lightning_send_completed(&payment_hash).unwrap());
        assert!(reopened.get_arkoor_send(&arkoor_quote).unwrap().is_some());
        assert_eq!(
            reopened
                .get_arkoor_quote(&arkoor_quote)
                .unwrap()
                .map(|quote| quote.amount_sat),
            Some(1_000)
        );
        assert!(reopened.is_arkoor_send_completed(&arkoor_quote).unwrap());
        assert_eq!(
            reopened.get_scan_cursor("test-scan").unwrap().as_deref(),
            Some("last-key")
        );
        drop(reopened);
        std::fs::remove_file(path).expect("remove test state store");
    }

    #[test]
    fn lightning_fee_cap_rejects_uncapped_and_excessive_fees() {
        assert!(BarkBackend::enforce_lightning_fee_cap(1, None).is_err());
        assert!(BarkBackend::enforce_lightning_fee_cap(11, Some(10)).is_err());
        assert!(BarkBackend::enforce_lightning_fee_cap(10, Some(10)).is_ok());
    }

    #[test]
    fn lightning_total_spent_uses_recorded_fee_not_preflight_fee() {
        let mut send = lightning_send(&"ab".repeat(32), &QuoteId::new().to_string());
        send.amount_sat = 1_000;
        send.preflight_fee_sat = 1;
        send.max_fee_sat = Some(50);
        send.fee_reconciled = true;
        send.state = LightningSendIntentState::Paid {
            fee_sat: 37,
            preimage: "preimage".to_string(),
            paid_at: 0,
        };

        let (status, payment_proof, total_spent) =
            BarkBackend::lightning_send_response_details(&send).expect("reconciled response");
        assert_eq!(status, MeltQuoteState::Paid);
        assert_eq!(payment_proof.as_deref(), Some("preimage"));
        assert_eq!(total_spent, 1_037);
    }

    #[test]
    fn lightning_status_mapping_is_conservative() {
        let mut send = lightning_send(&"ab".repeat(32), &QuoteId::new().to_string());
        send.state = LightningSendIntentState::Pending {
            fee_sat: 10,
            started_at: 0,
        };
        assert_eq!(
            BarkBackend::lightning_send_response_details(&send).unwrap(),
            (MeltQuoteState::Pending, None, 0)
        );

        send.state = LightningSendIntentState::Failed {
            reason: "no route".to_string(),
            fee_sat: Some(10),
            failed_at: 0,
        };
        assert_eq!(
            BarkBackend::lightning_send_response_details(&send).unwrap(),
            (MeltQuoteState::Unpaid, None, 0)
        );

        send.state = LightningSendIntentState::NeedsReview {
            reason: "ambiguous external result".to_string(),
            failed_at: 0,
        };
        assert_eq!(
            BarkBackend::lightning_send_response_details(&send).unwrap(),
            (MeltQuoteState::Pending, None, 0)
        );
    }

    #[test]
    fn legacy_lightning_send_fee_field_remains_readable() {
        let record: LightningSendIntentRecord = serde_json::from_value(serde_json::json!({
            "quote_id": QuoteId::new().to_string(),
            "payment_hash": "ab".repeat(32),
            "invoice": "invoice",
            "amount_sat": 1_000,
            "estimated_fee_sat": 1,
            "state": {
                "state": "needs_review",
                "reason": "test",
                "failed_at": 0
            }
        }))
        .expect("deserialize legacy lightning send");

        assert_eq!(record.preflight_fee_sat, 1);
        assert_eq!(record.max_fee_sat, None);
        assert!(!record.fee_reconciled);
    }

    #[test]
    fn legacy_paid_lightning_send_requires_fee_reconciliation() {
        let mut send = lightning_send(&"ab".repeat(32), &QuoteId::new().to_string());
        send.state = LightningSendIntentState::Paid {
            fee_sat: 1,
            preimage: "preimage".to_string(),
            paid_at: 0,
        };

        assert!(BarkBackend::lightning_send_needs_reconciliation(&send));
        assert!(BarkBackend::lightning_send_response_details(&send).is_err());

        send.fee_reconciled = true;
        assert!(!BarkBackend::lightning_send_needs_reconciliation(&send));
        assert!(BarkBackend::lightning_send_response_details(&send).is_ok());
    }

    #[test]
    fn reported_onchain_receive_is_not_reemitted_and_address_is_kept() {
        let store = test_store();
        let quote_id = QuoteId::new().to_string();
        let outpoint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0";
        let address = "bcrt1qexample";

        store
            .put_receive_address(&quote_id, address)
            .expect("store receive address");
        store
            .put_receive_intent(&finalized_receive(&quote_id, outpoint, 1_000))
            .expect("store receive intent");

        // First delivery: the finalized receive is reported once.
        let receive = store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)
            .expect("scan receives")
            .expect("unreported receive should be found");
        assert_eq!(receive.deposit_outpoint, outpoint);

        store
            .mark_onchain_receive_reported(outpoint)
            .expect("mark receive reported");

        // The reported marker suppresses re-emission...
        assert!(store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)
            .expect("scan receives")
            .is_none());
        // ...while the finalized intent stays for status re-checks...
        let finalized = store
            .finalized_receives_for_quote(&quote_id)
            .expect("finalized receives for quote");
        assert_eq!(finalized.len(), 1);
        // ...and the address mapping stays so later deposits are detected.
        let addresses = store.receive_addresses().expect("receive addresses");
        assert_eq!(addresses.get(&quote_id), Some(&address.to_string()));
    }

    #[test]
    fn capped_receive_scan_rotates_and_finds_backlog_tail() {
        let store = test_store();
        // Fill the store with more reported receives than one scan window
        // covers, then put an unreported receive behind them.
        for i in 0..(2 * MAX_RECEIVE_INTENTS_SCANNED_PER_TICK) {
            let outpoint = format!("{:064x}:0", i + 1);
            store
                .put_receive_intent(&finalized_receive(
                    &QuoteId::new().to_string(),
                    &outpoint,
                    100,
                ))
                .expect("store receive intent");
            store
                .mark_onchain_receive_reported(&outpoint)
                .expect("mark receive reported");
        }
        let tail_outpoint = format!("{:064x}:0", usize::MAX);
        store
            .put_receive_intent(&finalized_receive(
                &QuoteId::new().to_string(),
                &tail_outpoint,
                100,
            ))
            .expect("store receive intent");

        // First tick only sees a capped prefix of the reported backlog.
        assert!(store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)
            .expect("scan receives")
            .is_none());
        // Second tick resumes after the cursor and sees the next window.
        assert!(store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)
            .expect("scan receives")
            .is_none());
        // Third tick wraps around and reaches the unreported tail.
        let receive = store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)
            .expect("scan receives")
            .expect("unreported receive should be found after rotation");
        assert_eq!(receive.deposit_outpoint, tail_outpoint);

        // Reporting the receive and advancing the cursor keeps it suppressed.
        store
            .mark_onchain_receive_reported(&receive.deposit_outpoint)
            .expect("mark receive reported");
        store
            .advance_receive_scan_cursor(&receive.deposit_outpoint)
            .expect("advance scan cursor");
        assert!(store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)
            .expect("scan receives")
            .is_none());
    }

    #[test]
    fn stale_scan_cursor_recovers_to_full_coverage() {
        let store = test_store();
        // Point the cursor at a key that no longer exists; the scan must
        // recover and still find the unreported receive.
        store
            .put_scan_cursor(RECEIVE_SCAN_CURSOR_KEY, "ff:0")
            .expect("seed stale cursor");
        let outpoint = format!("{:064x}:0", 1);
        store
            .put_receive_intent(&finalized_receive(
                &QuoteId::new().to_string(),
                &outpoint,
                100,
            ))
            .expect("store receive intent");
        let receive = store
            .next_unreported_finalized_receive(MAX_RECEIVE_INTENTS_SCANNED_PER_TICK)
            .expect("scan receives")
            .expect("unreported receive should be found with stale cursor");
        assert_eq!(receive.deposit_outpoint, outpoint);
    }

    #[test]
    fn filtered_scan_resumes_after_a_missing_cursor_key() {
        let store = test_store();
        store
            .put_scan_cursor(ONCHAIN_SEND_RECONCILE_CURSOR_KEY, "quote-0001")
            .expect("seed filtered cursor");

        let records = vec![
            ("quote-0000".to_string(), ()),
            ("quote-0002".to_string(), ()),
            ("quote-0003".to_string(), ()),
        ];
        let window = store
            .rotated_records(ONCHAIN_SEND_RECONCILE_CURSOR_KEY, records, 1)
            .expect("rotate filtered reconciliation window");

        assert_eq!(window[0].0, "quote-0002");
    }

    #[test]
    fn capped_reconciliation_windows_rotate_through_entire_backlog() {
        let store = test_store();
        let record_count = 2 * MAX_INTENTS_RECONCILED_PER_TICK + 1;

        for index in 0..record_count {
            let quote_id = format!("quote-{index:04}");
            store
                .put_send(&quote_id, &onchain_send(&quote_id))
                .expect("store onchain send");

            let payment_hash = format!("{index:064x}");
            store
                .put_lightning_send(&payment_hash, &lightning_send(&payment_hash, &quote_id))
                .expect("store lightning send");
        }

        let mut onchain_seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let sends = store.sends().expect("list onchain sends");
            let window = store
                .rotated_records(
                    ONCHAIN_SEND_RECONCILE_CURSOR_KEY,
                    sends,
                    MAX_INTENTS_RECONCILED_PER_TICK,
                )
                .expect("rotate onchain reconciliation window");
            if let Some((last, _)) = window.last() {
                store
                    .put_scan_cursor(ONCHAIN_SEND_RECONCILE_CURSOR_KEY, last)
                    .expect("advance onchain reconciliation cursor");
            }
            onchain_seen.extend(window.into_iter().map(|(key, _)| key));
        }
        assert_eq!(onchain_seen.len(), record_count);

        let mut lightning_seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let sends = store.lightning_sends().expect("list lightning sends");
            let window = store
                .rotated_records(
                    LIGHTNING_SEND_RECONCILE_CURSOR_KEY,
                    sends,
                    MAX_INTENTS_RECONCILED_PER_TICK,
                )
                .expect("rotate lightning reconciliation window");
            if let Some((last, _)) = window.last() {
                store
                    .put_scan_cursor(LIGHTNING_SEND_RECONCILE_CURSOR_KEY, last)
                    .expect("advance lightning reconciliation cursor");
            }
            lightning_seen.extend(window.into_iter().map(|(key, _)| key));
        }
        assert_eq!(lightning_seen.len(), record_count);
    }

    #[test]
    fn onchain_reconciliation_only_selects_actionable_records() {
        let now = 1_000;
        let mut send = onchain_send("quote");
        assert!(!BarkBackend::onchain_send_needs_reconciliation(&send, now));

        send.state = OnchainSendIntentState::Attempting {
            attempt: 1,
            attempt_id: "attempt".to_string(),
            fee_sat: 10,
            started_at: now - SEND_ATTEMPT_REVIEW_SECS + 1,
        };
        assert!(!BarkBackend::onchain_send_needs_reconciliation(&send, now));

        send.state = OnchainSendIntentState::Attempting {
            attempt: 1,
            attempt_id: "attempt".to_string(),
            fee_sat: 10,
            started_at: now - SEND_ATTEMPT_REVIEW_SECS,
        };
        assert!(BarkBackend::onchain_send_needs_reconciliation(&send, now));

        send.state = OnchainSendIntentState::Broadcast {
            txid: "txid".to_string(),
            fee_sat: 10,
            broadcast_at: now,
        };
        assert!(BarkBackend::onchain_send_needs_reconciliation(&send, now));

        send.state = OnchainSendIntentState::Confirmed {
            txid: "txid".to_string(),
            fee_sat: 10,
            confirmed_at: now,
        };
        assert!(!BarkBackend::onchain_send_needs_reconciliation(&send, now));
    }

    #[test]
    fn reported_lightning_receive_keeps_quote_to_payment_hash_mapping() {
        let store = test_store();
        let quote_id = QuoteId::new().to_string();
        let payment_hash = "ab".repeat(32);

        store
            .put_lightning_receive_quote(&quote_id, &payment_hash)
            .expect("store receive quote");

        // Status checks before payment find no settled receive.
        assert!(store
            .get_lightning_receive_hash(&quote_id)
            .expect("get receive hash")
            .is_some());

        // Simulate the mint being notified via the event stream.
        let request_lookup_id =
            PaymentIdentifier::QuoteId(QuoteId::from_str(&quote_id).unwrap()).to_string();
        store
            .mark_lightning_receive_reported(&request_lookup_id)
            .expect("mark receive reported");

        assert!(store
            .is_lightning_receive_reported(&request_lookup_id)
            .expect("check reported"));
        // The quote -> payment hash mapping must survive reporting so later
        // status re-checks keep finding the paid invoice.
        assert_eq!(
            store
                .get_lightning_receive_hash(&quote_id)
                .expect("get receive hash"),
            Some(payment_hash)
        );
        assert!(store
            .lightning_receive_quotes()
            .expect("receive quotes")
            .iter()
            .any(|(stored_quote, _)| stored_quote == &quote_id));
    }
}
