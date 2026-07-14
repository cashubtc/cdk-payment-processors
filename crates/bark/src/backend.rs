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
use bark::onchain::{ChainSync, GetWalletTx, OnchainWallet, PreparePsbt, SignPsbt};
use bark::persist::sqlite::SqliteClient;
use bark::persist::BarkPersister;
use bitcoin::{Address, FeeRate, OutPoint, Psbt, Transaction, Txid};
use cdk_common::amount::Amount;
use cdk_common::nuts::nut_onchain::MeltQuoteOnchainFeeOption;
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

use crate::settings::BackendConfig;

const ONCHAIN_CONFIRMATIONS: u32 = 1;
const ONCHAIN_FEE_INDEX: u32 = 0;
const ONCHAIN_ESTIMATED_BLOCKS: u32 = 6;

/// Ark payment processor backend using the Bark wallet library
#[derive(Clone)]
pub struct ArkBackend {
    wallet: Arc<bark::Wallet>,
    onchain_wallet: Arc<tokio::sync::Mutex<OnchainWallet>>,
    onchain_send_lock: Arc<tokio::sync::Mutex<()>>,
    lightning_send_lock: Arc<tokio::sync::Mutex<()>>,
    state_store: Arc<ArkStateStore>,
    network: bitcoin::Network,
    wait_invoice_active: Arc<AtomicBool>,
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

const RETRY_BACKOFF_SECS: u64 = 30;
const SEND_ATTEMPT_REVIEW_SECS: u64 = 60;

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
        attempt_id: String,
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
    estimated_fee_sat: u64,
    state: LightningSendIntentState,
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

struct ArkStateStore {
    db: Database,
}

impl ArkStateStore {
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
        }
        tx.commit()?;
        Ok(())
    }

    fn store_error(e: impl std::fmt::Display) -> cdk_common::payment::Error {
        cdk_common::payment::Error::Custom(format!("Onchain state store error: {}", e))
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

    fn next_unreported_finalized_receive(
        &self,
    ) -> Result<Option<OnchainReceiveIntentRecord>, cdk_common::payment::Error> {
        for intent in self.receive_intents()? {
            if matches!(intent.state, OnchainReceiveIntentState::Finalized { .. })
                && !self.is_receive_reported(&intent.deposit_outpoint)?
            {
                return Ok(Some(intent));
            }
        }
        Ok(None)
    }

    fn mark_receive_reported(&self, outpoint: &str) -> Result<(), cdk_common::payment::Error> {
        let tx = self.db.begin_write().map_err(Self::store_error)?;
        {
            let mut table = tx
                .open_table(REPORTED_RECEIVES_TABLE)
                .map_err(Self::store_error)?;
            table.insert(outpoint, "1").map_err(Self::store_error)?;
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

    fn lightning_receive_quote_for_hash(
        &self,
        payment_hash: &str,
    ) -> Result<Option<String>, cdk_common::payment::Error> {
        let tx = self.db.begin_read().map_err(Self::store_error)?;
        let table = tx
            .open_table(LIGHTNING_RECEIVE_QUOTES_TABLE)
            .map_err(Self::store_error)?;
        for entry in table.iter().map_err(Self::store_error)? {
            let (quote_id, stored_hash) = entry.map_err(Self::store_error)?;
            if stored_hash.value() == payment_hash {
                return Ok(Some(quote_id.value().to_string()));
            }
        }
        Ok(None)
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
}

struct ScopedBoard<'a> {
    inner: &'a mut OnchainWallet,
    outpoint: OutPoint,
}

impl PreparePsbt for ScopedBoard<'_> {
    fn prepare_tx(
        &mut self,
        destinations: &[(Address, bitcoin::Amount)],
        fee_rate: FeeRate,
    ) -> anyhow::Result<Psbt> {
        let mut builder = self.inner.build_tx();
        builder.ordering(TxOrdering::Untouched);
        builder.add_utxo(self.outpoint)?;
        builder.manually_selected_only();
        for (dest, amount) in destinations {
            builder.add_recipient(dest.script_pubkey(), *amount);
        }
        builder.fee_rate(fee_rate);
        builder.finish().map_err(Into::into)
    }

    fn prepare_drain_tx(
        &mut self,
        destination: Address,
        fee_rate: FeeRate,
    ) -> anyhow::Result<Psbt> {
        let mut builder = self.inner.build_tx();
        builder.ordering(TxOrdering::Untouched);
        builder.add_utxo(self.outpoint)?;
        builder.manually_selected_only();
        builder.drain_to(destination.script_pubkey());
        builder.fee_rate(fee_rate);
        builder.finish().map_err(Into::into)
    }
}

#[async_trait]
impl SignPsbt for ScopedBoard<'_> {
    async fn finish_tx(&mut self, psbt: Psbt) -> anyhow::Result<Transaction> {
        self.inner.finish_tx(psbt).await
    }
}

impl GetWalletTx for ScopedBoard<'_> {
    fn get_wallet_tx(&self, txid: Txid) -> Option<Arc<Transaction>> {
        self.inner.get_wallet_tx(txid)
    }

    fn get_wallet_tx_confirmed_block(
        &self,
        txid: Txid,
    ) -> anyhow::Result<Option<bitcoin_ext::BlockRef>> {
        self.inner.get_wallet_tx_confirmed_block(txid)
    }
}

impl ArkBackend {
    /// Create a new Ark backend with initialized wallet
    pub async fn new(config: &BackendConfig) -> anyhow::Result<Self> {
        info!("Initializing Ark backend");

        // Parse the mnemonic
        let mnemonic = config
            .mnemonic
            .parse::<bip39::Mnemonic>()
            .map_err(|e| anyhow::anyhow!("Invalid mnemonic: {}", e))?;

        // Parse the network
        let network = match config.network.to_lowercase().as_str() {
            "mainnet" => bitcoin::Network::Bitcoin,
            "testnet" => bitcoin::Network::Testnet,
            "signet" => bitcoin::Network::Signet,
            "regtest" => bitcoin::Network::Regtest,
            _ => {
                warn!("Unknown network '{}', defaulting to Signet", config.network);
                bitcoin::Network::Signet
            }
        };

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

        let mut onchain_wallet =
            OnchainWallet::load_or_create(network, mnemonic.to_seed(""), db.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load onchain wallet: {}", e))?;

        // Try to open existing wallet first, fall back to creating new one
        let wallet = match bark::Wallet::open_with_onchain(
            &mnemonic,
            db.clone(),
            &onchain_wallet,
            bark_config.clone(),
        )
        .await
        {
            Ok(wallet) => {
                info!("Opened existing Ark wallet");
                wallet
            }
            Err(e) => {
                info!("Creating new Ark wallet (open failed: {})", e);
                bark::Wallet::create_with_onchain(
                    &mnemonic,
                    network,
                    bark_config,
                    db,
                    &onchain_wallet,
                    false,
                )
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create wallet: {}", e))?
            }
        };

        onchain_wallet
            .sync(&wallet.chain)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to sync onchain wallet: {}", e))?;

        let state_store = Arc::new(
            ArkStateStore::open(data_dir.join("onchain_state.redb"))
                .map_err(|e| anyhow::anyhow!("Failed to open onchain state store: {}", e))?,
        );

        info!("Ark backend initialized successfully");

        Ok(Self {
            wallet: Arc::new(wallet),
            onchain_wallet: Arc::new(tokio::sync::Mutex::new(onchain_wallet)),
            onchain_send_lock: Arc::new(tokio::sync::Mutex::new(())),
            lightning_send_lock: Arc::new(tokio::sync::Mutex::new(())),
            state_store,
            network,
            wait_invoice_active: Arc::new(AtomicBool::new(false)),
        })
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
        if let Err(e) = self.wallet.sync_pending_boards().await {
            debug!("Failed to sync pending boards: {}", e);
        }

        let tip = self.wallet.chain.tip().await.map_err(|e| {
            cdk_common::payment::Error::Custom(format!("Failed to get chain tip: {}", e))
        })?;

        let mut onchain = self.onchain_wallet.lock().await;
        onchain.sync(&self.wallet.chain).await.map_err(|e| {
            cdk_common::payment::Error::Custom(format!("Failed to sync onchain wallet: {}", e))
        })?;

        self.recover_preparing_receive_boards(&onchain).await?;
        self.finalize_spendable_receive_boards().await?;
        self.detect_confirmed_receive_deposits(&onchain, tip)
            .await?;
        self.start_ready_receive_boards(&mut onchain).await
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

            QuoteId::from_str(quote_id_str).map_err(|e| {
                cdk_common::payment::Error::Custom(format!(
                    "Invalid stored quote id {}: {}",
                    quote_id_str, e
                ))
            })?;

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

    async fn start_ready_receive_boards(
        &self,
        onchain: &mut OnchainWallet,
    ) -> Result<(), cdk_common::payment::Error> {
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

            let outpoint = OutPoint::from_str(&intent.deposit_outpoint).map_err(|e| {
                cdk_common::payment::Error::Custom(format!(
                    "Invalid stored deposit outpoint {}: {}",
                    intent.deposit_outpoint, e
                ))
            })?;

            let attempt_id = uuid::Uuid::new_v4().to_string();
            let started_at = Self::unix_now();
            let mut preparing = intent.clone();
            preparing.state = OnchainReceiveIntentState::BoardPreparing {
                attempt,
                attempt_id,
                started_at,
            };
            self.state_store.put_receive_intent(&preparing)?;

            let board_result = {
                let mut scoped_board = ScopedBoard {
                    inner: onchain,
                    outpoint,
                };
                self.wallet.board_all(&mut scoped_board).await
            };

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
                    } else if onchain
                        .list_unspent()
                        .iter()
                        .any(|output| output.outpoint == outpoint)
                    {
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

            let outpoint = OutPoint::from_str(&intent.deposit_outpoint).map_err(|e| {
                cdk_common::payment::Error::Custom(format!(
                    "Invalid stored deposit outpoint {}: {}",
                    intent.deposit_outpoint, e
                ))
            })?;

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
                self.state_store.mark_receive_reported(outpoint)?;
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

        let Some(receive) = self.state_store.next_unreported_finalized_receive()? else {
            return Ok(None);
        };

        let quote_id = QuoteId::from_str(&receive.quote_id).map_err(|e| {
            cdk_common::payment::Error::Custom(format!(
                "Invalid stored quote id {}: {}",
                receive.quote_id, e
            ))
        })?;

        let OnchainReceiveIntentState::Finalized {
            board_txid,
            amount_sat,
            ..
        } = receive.state
        else {
            return Ok(None);
        };

        self.state_store
            .mark_receive_reported(&receive.deposit_outpoint)?;

        Ok(Some(Event::PaymentReceived(WaitPaymentResponse {
            payment_identifier: PaymentIdentifier::QuoteId(quote_id),
            payment_amount: Amount::new(amount_sat, CurrencyUnit::Sat),
            payment_id: board_txid,
        })))
    }

    async fn next_onchain_send_event(&self) -> Result<Option<Event>, cdk_common::payment::Error> {
        self.reconcile_onchain_sends().await?;

        for (quote_id_str, send) in self.state_store.sends()? {
            if self.state_store.is_send_completed(&quote_id_str)? {
                continue;
            }

            let OnchainSendIntentState::Confirmed { txid, fee_sat, .. } = &send.state else {
                continue;
            };

            let quote_id = QuoteId::from_str(&quote_id_str).map_err(|e| {
                cdk_common::payment::Error::Custom(format!(
                    "Invalid stored quote id {}: {}",
                    quote_id_str, e
                ))
            })?;

            self.state_store.mark_send_completed(&quote_id_str)?;

            let total_spent = send.amount_sat.saturating_add(*fee_sat);
            return Ok(Some(Event::PaymentSuccessful {
                quote_id: quote_id.clone(),
                details: MakePaymentResponse {
                    payment_lookup_id: PaymentIdentifier::QuoteId(quote_id),
                    payment_proof: Some(txid.clone()),
                    status: MeltQuoteState::Paid,
                    total_spent: Amount::new(total_spent, CurrencyUnit::Sat),
                },
            }));
        }

        Ok(None)
    }

    async fn check_onchain_send(
        &self,
        quote_id: &QuoteId,
        mark_completed: bool,
    ) -> Result<Option<MakePaymentResponse>, cdk_common::payment::Error> {
        self.reconcile_onchain_sends().await?;

        let quote_id_str = quote_id.to_string();
        let send = self.state_store.get_send(&quote_id_str)?;
        let Some(send) = send else {
            return Ok(None);
        };

        Ok(Some(self.onchain_send_response(
            quote_id,
            &send,
            mark_completed,
        )?))
    }

    async fn reconcile_onchain_sends(&self) -> Result<(), cdk_common::payment::Error> {
        if let Err(e) = self.wallet.sync_pending_offboards().await {
            debug!("Failed to sync pending offboards: {}", e);
        }

        let now = Self::unix_now();
        for (quote_id_str, send) in self.state_store.sends()? {
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
                    self.state_store.put_send(&quote_id_str, &needs_review)?;
                    warn!(
                        "Marked onchain send quote {} as needs_review after interrupted Bark send_onchain",
                        quote_id_str
                    );
                }
                OnchainSendIntentState::Broadcast { txid, fee_sat, .. } => {
                    let parsed_txid = Txid::from_str(txid).map_err(|e| {
                        cdk_common::payment::Error::Custom(format!(
                            "Invalid stored offboard txid {}: {}",
                            txid, e
                        ))
                    })?;

                    match self.wallet.chain.tx_status(parsed_txid).await {
                        Ok(bitcoin_ext::TxStatus::Confirmed(_)) => {
                            let mut confirmed = send.clone();
                            confirmed.state = OnchainSendIntentState::Confirmed {
                                txid: txid.clone(),
                                fee_sat: *fee_sat,
                                confirmed_at: now,
                            };
                            self.state_store.put_send(&quote_id_str, &confirmed)?;
                            info!("Confirmed onchain send {} for quote {}", txid, quote_id_str);
                        }
                        Ok(bitcoin_ext::TxStatus::Mempool)
                        | Ok(bitcoin_ext::TxStatus::NotFound) => {}
                        Err(e) => {
                            debug!("Failed to check onchain tx status for {}: {}", txid, e);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
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

    async fn reconcile_lightning_sends(&self) -> Result<(), cdk_common::payment::Error> {
        for (payment_hash, _) in self.state_store.lightning_sends()? {
            self.reconcile_lightning_send(&payment_hash).await?;
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
            Ok(Some(send)) => {
                let updated = Self::lightning_intent_from_bark_send(intent, &send);
                self.state_store
                    .put_lightning_send(payment_hash_hex, &updated)?;
                Ok(Some(updated))
            }
            Ok(None) => Ok(Some(intent)),
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

        for (payment_hash, send) in self.state_store.lightning_sends()? {
            if self
                .state_store
                .is_lightning_send_completed(&payment_hash)?
            {
                continue;
            }

            let quote_id = QuoteId::from_str(&send.quote_id).map_err(|e| {
                cdk_common::payment::Error::Custom(format!(
                    "Invalid stored quote id {}: {}",
                    send.quote_id, e
                ))
            })?;

            match &send.state {
                LightningSendIntentState::Paid { .. } => {
                    self.state_store
                        .mark_lightning_send_completed(&payment_hash)?;
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
                    return Ok(Some(Event::PaymentFailed {
                        quote_id,
                        reason: reason.clone(),
                    }));
                }
                _ => {}
            }
        }

        Ok(None)
    }

    fn lightning_send_response_with_lookup(
        &self,
        send: &LightningSendIntentRecord,
        mark_completed: bool,
        payment_lookup_id: PaymentIdentifier,
    ) -> Result<MakePaymentResponse, cdk_common::payment::Error> {
        let (status, payment_proof, total_spent) = match &send.state {
            LightningSendIntentState::Paid {
                fee_sat, preimage, ..
            } => {
                if mark_completed {
                    self.state_store
                        .mark_lightning_send_completed(&send.payment_hash)?;
                }
                (
                    MeltQuoteState::Paid,
                    Some(preimage.clone()),
                    send.amount_sat.saturating_add(*fee_sat),
                )
            }
            LightningSendIntentState::Failed { .. } => (MeltQuoteState::Unpaid, None, 0),
            LightningSendIntentState::Attempting { .. }
            | LightningSendIntentState::Pending { .. }
            | LightningSendIntentState::NeedsReview { .. } => (MeltQuoteState::Pending, None, 0),
        };

        Ok(MakePaymentResponse {
            payment_lookup_id,
            payment_proof,
            status,
            total_spent: Amount::new(total_spent, CurrencyUnit::Sat),
        })
    }

    fn lightning_intent_from_bark_send(
        mut intent: LightningSendIntentRecord,
        send: &bark::persist::models::LightningSend,
    ) -> LightningSendIntentRecord {
        let fee_sat = send.fee.to_sat();
        intent.amount_sat = send.amount.to_sat();
        intent.state = match (&send.preimage, send.finished_at) {
            (Some(preimage), Some(_)) => LightningSendIntentState::Paid {
                fee_sat,
                preimage: hex::encode(preimage.as_ref()),
                paid_at: Self::unix_now(),
            },
            (None, Some(_)) => LightningSendIntentState::Failed {
                reason: "Lightning payment failed".to_string(),
                fee_sat: Some(fee_sat),
                failed_at: Self::unix_now(),
            },
            _ => LightningSendIntentState::Pending {
                fee_sat,
                started_at: Self::unix_now(),
            },
        };
        intent
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

    fn estimated_lightning_fee_sat(amount_sat: u64) -> u64 {
        std::cmp::max(1, amount_sat / 1000)
    }

    /// Convert bitcoin::Amount to CDK Amount (instance method)
    fn btc_amount_to_cdk(&self, amount: bitcoin::Amount) -> Amount<CurrencyUnit> {
        Amount::new(amount.to_sat(), CurrencyUnit::Sat)
    }

    /// Convert bitcoin::Amount to CDK Amount (static method)
    fn btc_amount_to_cdk_static(amount: bitcoin::Amount) -> Amount<CurrencyUnit> {
        Amount::new(amount.to_sat(), CurrencyUnit::Sat)
    }

    /// Get zero CDK amount
    fn cdk_amount_zero() -> Amount<CurrencyUnit> {
        Amount::new(0, CurrencyUnit::Sat)
    }

    async fn check_lightning_receive(
        &self,
        payment_identifier: PaymentIdentifier,
        payment_hash: PaymentHash,
        mark_reported: bool,
    ) -> Result<Vec<WaitPaymentResponse>, cdk_common::payment::Error> {
        let receive = self
            .wallet
            .lightning_receive_status(payment_hash)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to check receive status: {}", e))
            })?;

        if let Some(receive) = receive {
            if receive.finished_at.is_some() {
                let amount = receive
                    .invoice
                    .amount_milli_satoshis()
                    .map(|msat| self.btc_amount_to_cdk(bitcoin::Amount::from_sat(msat / 1000)))
                    .unwrap_or(Self::cdk_amount_zero());

                let payment_hash_bytes: [u8; 32] = payment_hash.into();
                if mark_reported {
                    self.state_store
                        .mark_lightning_receive_reported(&payment_identifier.to_string())?;
                }
                return Ok(vec![WaitPaymentResponse {
                    payment_identifier,
                    payment_amount: amount,
                    payment_id: hex::encode(payment_hash_bytes),
                }]);
            }
        }

        Ok(vec![])
    }

    fn unix_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default()
    }
}

#[async_trait]
impl MintPayment for ArkBackend {
    type Err = cdk_common::payment::Error;

    async fn get_settings(&self) -> Result<SettingsResponse, Self::Err> {
        debug!("Getting Ark wallet settings");
        Ok(SettingsResponse {
            unit: "sat".to_string(),
            bolt11: Some(Bolt11Settings {
                mpp: false,
                amountless: false,
                invoice_description: true,
            }),
            bolt12: None,
            onchain: Some(OnchainSettings {
                confirmations: ONCHAIN_CONFIRMATIONS,
                min_receive_amount_sat: 1,
                min_send_amount_sat: 1,
            }),
            custom: Default::default(),
        })
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
                    let mut onchain = self.onchain_wallet.lock().await;
                    onchain.sync(&self.wallet.chain).await.map_err(|e| {
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
            .bolt11_invoice(amount, bolt11_options.description)
            .await
            .map_err(|e| {
                cdk_common::payment::Error::Custom(format!("Failed to create invoice: {}", e))
            })?;

        // Extract payment hash from the invoice - bark returns lightning_invoice::Bolt11Invoice
        let payment_hash_bytes: [u8; 32] = *invoice.payment_hash().as_ref();
        let payment_hash_hex = hex::encode(payment_hash_bytes);
        let quote_id = QuoteId::new_uuid();
        self.state_store
            .put_lightning_receive_quote(&quote_id.to_string(), &payment_hash_hex)?;

        // Get expiry - convert Duration to seconds
        let expiry = Some(invoice.expiry_time().as_secs());

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

                let amount_msat = invoice.amount_milli_satoshis().ok_or_else(|| {
                    cdk_common::payment::Error::Custom("Invoice has no amount".to_string())
                })?;
                let amount_sat = amount_msat / 1000;
                let fee_sats = Self::estimated_lightning_fee_sat(amount_sat);

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
            OutgoingPaymentOptions::Onchain(opts) => {
                if !matches!(opts.fee_index, None | Some(ONCHAIN_FEE_INDEX)) {
                    return Err(cdk_common::payment::Error::Custom(format!(
                        "Unsupported onchain fee_index {:?}",
                        opts.fee_index
                    )));
                }

                let _send_guard = self.onchain_send_lock.lock().await;
                self.reconcile_onchain_sends().await?;

                let quote_id_str = opts.quote_id.to_string();
                if let Some(existing_send) = self.state_store.get_send(&quote_id_str)? {
                    return self.onchain_send_response(&opts.quote_id, &existing_send, false);
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
        let estimated_fee_sat = std::cmp::max(1, amount_sat / 1000);

        if let Some(max_fee) = bolt11_options.max_fee_amount.as_ref() {
            let max_fee_sat = max_fee.clone().to_u64();
            if estimated_fee_sat > max_fee_sat {
                return Err(cdk_common::payment::Error::Custom(format!(
                    "Estimated lightning fee {} sat exceeds max fee {} sat",
                    estimated_fee_sat, max_fee_sat
                )));
            }
        }

        let _lightning_send_guard = self.lightning_send_lock.lock().await;
        if let Some((existing_payment_hash, _)) =
            self.state_store.lightning_send_for_quote(&quote_id_str)?
        {
            if let Some(existing_send) = self
                .reconcile_lightning_send(&existing_payment_hash)
                .await?
            {
                return self.lightning_send_response_with_lookup(
                    &existing_send,
                    false,
                    payment_lookup_id.clone(),
                );
            }
        }
        if let Some(existing_send) = self.reconcile_lightning_send(&payment_hash_hex).await? {
            return self.lightning_send_response_with_lookup(
                &existing_send,
                false,
                payment_lookup_id.clone(),
            );
        }

        let invoice_str = invoice.to_string();
        let mut send_intent = LightningSendIntentRecord {
            quote_id: bolt11_options.quote_id.to_string(),
            payment_hash: payment_hash_hex.clone(),
            invoice: invoice_str.clone(),
            amount_sat,
            estimated_fee_sat,
            state: LightningSendIntentState::Attempting {
                attempt: 1,
                attempt_id: uuid::Uuid::new_v4().to_string(),
                started_at: Self::unix_now(),
            },
        };
        self.state_store
            .put_lightning_send(&payment_hash_hex, &send_intent)?;

        let lightning_send = match self
            .wallet
            .pay_lightning_invoice(invoice_str.as_str(), None)
            .await
        {
            Ok(lightning_send) => lightning_send,
            Err(e) => {
                let reason = e.to_string();
                match self
                    .wallet
                    .check_lightning_payment(PaymentHash::from(payment_hash), false)
                    .await
                {
                    Ok(Some(recovered_send)) => {
                        let recovered =
                            Self::lightning_intent_from_bark_send(send_intent, &recovered_send);
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
        };

        let updated_send = Self::lightning_intent_from_bark_send(send_intent, &lightning_send);
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

        // Create a stream that polls for incoming payments
        let stream = stream::unfold(
            (backend, wallet, active, false),
            |(backend, wallet, active, _)| async move {
                // Check if we should stop
                if !active.load(Ordering::SeqCst) {
                    return None;
                }

                // Wait for the polling interval
                tokio::time::sleep(Duration::from_secs(5)).await;

                // Try to claim all lightning receives (non-blocking)
                if let Err(e) = wallet.try_claim_all_lightning_receives(false).await {
                    debug!("Failed to claim lightning receives: {}", e);
                }

                // Get pending lightning receives
                let pending = match wallet.pending_lightning_receives().await {
                    Ok(pending) => pending,
                    Err(e) => {
                        debug!("Failed to get pending receives: {}", e);
                        Vec::new()
                    }
                };

                // Check for completed receives
                for receive in pending {
                    // If the receive has finished_at set, it's complete
                    if receive.finished_at.is_some() {
                        let payment_hash = receive.payment_hash;
                        let payment_hash_bytes: [u8; 32] = payment_hash.into();
                        let payment_hash_hex = hex::encode(payment_hash_bytes);
                        let payment_identifier = match backend
                            .state_store
                            .lightning_receive_quote_for_hash(&payment_hash_hex)
                        {
                            Ok(Some(quote_id_str)) => match QuoteId::from_str(&quote_id_str) {
                                Ok(quote_id) => PaymentIdentifier::QuoteId(quote_id),
                                Err(e) => {
                                    debug!(
                                        "Invalid stored lightning receive quote id {}: {}",
                                        quote_id_str, e
                                    );
                                    PaymentIdentifier::PaymentHash(payment_hash_bytes)
                                }
                            },
                            Ok(None) => PaymentIdentifier::PaymentHash(payment_hash_bytes),
                            Err(e) => {
                                debug!("Failed to look up lightning receive quote id: {}", e);
                                PaymentIdentifier::PaymentHash(payment_hash_bytes)
                            }
                        };
                        let request_lookup_id = payment_identifier.to_string();
                        match backend
                            .state_store
                            .is_lightning_receive_reported(&request_lookup_id)
                        {
                            Ok(true) => continue,
                            Ok(false) => {}
                            Err(e) => {
                                debug!("Failed to check lightning receive report state: {}", e);
                            }
                        }
                        let amount = receive
                            .invoice
                            .amount_milli_satoshis()
                            .map(|msat| {
                                Self::btc_amount_to_cdk_static(bitcoin::Amount::from_sat(
                                    msat / 1000,
                                ))
                            })
                            .unwrap_or(Self::cdk_amount_zero());

                        if let Err(e) = backend
                            .state_store
                            .mark_lightning_receive_reported(&request_lookup_id)
                        {
                            debug!("Failed to mark lightning receive reported: {}", e);
                        }
                        let event = Event::PaymentReceived(WaitPaymentResponse {
                            payment_identifier,
                            payment_amount: amount,
                            payment_id: payment_hash_hex,
                        });

                        return Some((Some(event), (backend, wallet, active, false)));
                    }
                }

                match backend.next_onchain_receive_event().await {
                    Ok(Some(event)) => {
                        return Some((Some(event), (backend, wallet, active, false)));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!("Failed to process onchain receives: {}", e);
                    }
                }

                match backend.next_lightning_send_event().await {
                    Ok(Some(event)) => {
                        return Some((Some(event), (backend, wallet, active, false)));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!("Failed to process lightning sends: {}", e);
                    }
                }

                match backend.next_onchain_send_event().await {
                    Ok(Some(event)) => {
                        return Some((Some(event), (backend, wallet, active, false)));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!("Failed to process onchain sends: {}", e);
                    }
                }

                Some((None, (backend, wallet, active, false)))
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
            if let Some((payment_hash, _)) =
                self.state_store.lightning_send_for_quote(&quote_id_str)?
            {
                if let Some(send) = self.reconcile_lightning_send(&payment_hash).await? {
                    return self.lightning_send_response_with_lookup(
                        &send,
                        true,
                        PaymentIdentifier::QuoteId(quote_id.clone()),
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
