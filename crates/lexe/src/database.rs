//! Lexe quote database.
//!
//! Maps BOLT11 payment hashes to invoice strings and Lexe payment indexes so
//! that repeated `make_payment` / status checks never submit a payment twice.
//!
//! An attempt and its event owner are committed before submission. An
//! ambiguous attempt is reconciled rather than submitted again, even if the
//! remote index was never received.

use anyhow::Result;
use cdk_common::{CurrencyUnit, QuoteId};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

/// BOLT11 invoices we created, keyed by payment hash.
const MINT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("mint_quotes");

/// Lexe payment index (string form of `PaymentCreatedIndex`) for each created
/// invoice, keyed by payment hash.
const MINT_PAYMENT_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("mint_payment_ids");

/// Outgoing BOLT11 invoices we quoted, keyed by payment hash.
const MELT_QUOTES_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("melt_quotes");

/// Lexe payment index for each outgoing payment, keyed by payment hash.
const MELT_PAYMENT_IDS_TABLE: TableDefinition<&[u8; 32], &str> =
    TableDefinition::new("melt_payment_ids");

const MELT_ATTEMPTS_TABLE: TableDefinition<&[u8; 32], &str> = TableDefinition::new("melt_attempts");

/// Durable submission intent. Its owner is immutable, including on retries.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MeltAttempt {
    /// Event owner for this melt.
    pub quote_id: Option<QuoteId>,
    /// Unit in which the attempt was requested.
    pub unit: CurrencyUnit,
    /// True only when submission was definitively rejected, before acceptance.
    pub submission_rejected: bool,
}

/// Database wrapper for quote-to-payment mappings.
#[derive(Clone)]
pub struct QuoteDatabase {
    db: Arc<Database>,
}

impl QuoteDatabase {
    /// Create a new database instance or open an existing one.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;

        let write_txn = db.begin_write()?;
        {
            write_txn.open_table(MINT_QUOTES_TABLE)?;
            write_txn.open_table(MINT_PAYMENT_IDS_TABLE)?;
            write_txn.open_table(MELT_QUOTES_TABLE)?;
            write_txn.open_table(MELT_PAYMENT_IDS_TABLE)?;
            write_txn.open_table(MELT_ATTEMPTS_TABLE)?;
        }
        write_txn.commit()?;

        tracing::info!("Lexe quote database initialized");

        Ok(Self { db: Arc::new(db) })
    }

    /// Store a created mint invoice and its Lexe payment index in a single
    /// transaction, so a crash between the two writes cannot leave an
    /// invoice without a payment index.
    pub fn insert_mint_quote_and_payment_id(
        &self,
        payment_hash: &[u8; 32],
        payment_request: &str,
        payment_index: &str,
    ) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut quotes = write_txn.open_table(MINT_QUOTES_TABLE)?;
            quotes.insert(payment_hash, payment_request)?;
            let mut payment_ids = write_txn.open_table(MINT_PAYMENT_IDS_TABLE)?;
            payment_ids.insert(payment_hash, payment_index)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Get the mint invoice we created for this payment hash.
    pub fn get_mint_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MINT_QUOTES_TABLE, payment_hash)
    }

    /// Get the Lexe payment index for a created invoice.
    pub fn get_mint_payment_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MINT_PAYMENT_IDS_TABLE, payment_hash)
    }

    /// Store an outgoing (melt) invoice.
    pub fn insert_melt_quote(&self, payment_hash: &[u8; 32], payment_request: &str) -> Result<()> {
        self.insert_mapping(MELT_QUOTES_TABLE, payment_hash, payment_request)
    }

    /// Get the stored outgoing invoice for this payment hash.
    pub fn get_melt_quote(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MELT_QUOTES_TABLE, payment_hash)
    }

    /// Store the Lexe payment index for an outgoing payment.
    pub fn insert_melt_payment_id(
        &self,
        payment_hash: &[u8; 32],
        payment_index: &str,
    ) -> Result<()> {
        self.insert_mapping(MELT_PAYMENT_IDS_TABLE, payment_hash, payment_index)
    }

    /// Get the Lexe payment index for an outgoing payment.
    pub fn get_melt_payment_id(&self, payment_hash: &[u8; 32]) -> Result<Option<String>> {
        self.get_mapping(MELT_PAYMENT_IDS_TABLE, payment_hash)
    }

    /// Claim an invoice for submission and persist its event owner atomically.
    /// Returns false when another call already claimed it; callers must recover
    /// that attempt and must not submit again.
    pub fn begin_melt_attempt(
        &self,
        payment_hash: &[u8; 32],
        quote_id: &QuoteId,
        unit: &CurrencyUnit,
    ) -> Result<bool> {
        let write_txn = self.db.begin_write()?;
        {
            let mut attempts = write_txn.open_table(MELT_ATTEMPTS_TABLE)?;
            if attempts.get(payment_hash)?.is_some() {
                return Ok(false);
            }
            let value = serde_json::to_string(&MeltAttempt {
                quote_id: Some(quote_id.clone()),
                unit: unit.clone(),
                submission_rejected: false,
            })?;
            attempts.insert(payment_hash, value.as_str())?;
        }
        write_txn.commit()?;
        Ok(true)
    }

    /// Read the durable attempt, independently of whether its index is known.
    pub fn get_melt_attempt(&self, payment_hash: &[u8; 32]) -> Result<Option<MeltAttempt>> {
        self.get_mapping(MELT_ATTEMPTS_TABLE, payment_hash)?
            .map(|raw| serde_json::from_str(&raw).map_err(Into::into))
            .transpose()
    }

    /// Persist a definite submission rejection without releasing the claim or
    /// changing its owner. Never downgrade an attempt with a known remote index.
    pub fn reject_melt_attempt(&self, payment_hash: &[u8; 32]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let indexes = write_txn.open_table(MELT_PAYMENT_IDS_TABLE)?;
            anyhow::ensure!(
                indexes.get(payment_hash)?.is_none(),
                "Cannot reject an attempt with a remote payment index"
            );
            let mut attempts = write_txn.open_table(MELT_ATTEMPTS_TABLE)?;
            let mut attempt: MeltAttempt = {
                let raw = attempts
                    .get(payment_hash)?
                    .ok_or_else(|| anyhow::anyhow!("Outgoing attempt not found"))?;
                serde_json::from_str(raw.value())?
            };
            attempt.submission_rejected = true;
            let value = serde_json::to_string(&attempt)?;
            attempts.insert(payment_hash, value.as_str())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn insert_mapping(
        &self,
        definition: TableDefinition<&[u8; 32], &str>,
        payment_hash: &[u8; 32],
        value: &str,
    ) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(definition)?;
            table.insert(payment_hash, value)?;
        }
        write_txn.commit()?;
        tracing::debug!(
            "Inserted lexe mapping: {} -> {}",
            hex::encode(payment_hash),
            value
        );
        Ok(())
    }

    fn get_mapping(
        &self,
        definition: TableDefinition<&[u8; 32], &str>,
        payment_hash: &[u8; 32],
    ) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(definition)?;
        let result = table.get(payment_hash)?;
        Ok(result.map(|value| value.value().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db_path() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cdk-lexe-quotes-{}-{unique}.redb",
            std::process::id()
        ))
    }

    #[test]
    fn persists_all_quote_mappings() {
        let path = test_db_path();
        let hash = [42_u8; 32];
        let quote_id = QuoteId::new();

        {
            let db = QuoteDatabase::new(&path).expect("create quote database");
            db.insert_mint_quote_and_payment_id(&hash, "lnbc1incoming-invoice", "0001-ln_aabbcc")
                .expect("insert mint quote and payment index");
            db.insert_melt_quote(&hash, "lnbc1outgoing-invoice")
                .expect("insert melt invoice");
            db.insert_melt_payment_id(&hash, "0002-ln_ddddeeff")
                .expect("insert melt payment index");
            db.begin_melt_attempt(&hash, &quote_id, &CurrencyUnit::Sat)
                .expect("begin melt attempt");
        }

        let db = QuoteDatabase::new(&path).expect("reopen quote database");
        assert_eq!(
            db.get_mint_quote(&hash).expect("get mint invoice"),
            Some("lnbc1incoming-invoice".to_string())
        );
        assert_eq!(
            db.get_mint_payment_id(&hash)
                .expect("get mint payment index"),
            Some("0001-ln_aabbcc".to_string())
        );
        assert_eq!(
            db.get_melt_quote(&hash).expect("get melt invoice"),
            Some("lnbc1outgoing-invoice".to_string())
        );
        assert_eq!(
            db.get_melt_payment_id(&hash)
                .expect("get melt payment index"),
            Some("0002-ln_ddddeeff".to_string())
        );
        assert_eq!(
            db.get_melt_attempt(&hash).expect("get melt attempt"),
            Some(MeltAttempt {
                quote_id: Some(quote_id),
                unit: CurrencyUnit::Sat,
                submission_rejected: false,
            })
        );

        // Unknown hashes return None.
        let other = [7_u8; 32];
        assert_eq!(db.get_mint_quote(&other).expect("miss"), None);
        assert_eq!(db.get_melt_payment_id(&other).expect("miss"), None);

        drop(db);
        std::fs::remove_file(path).expect("remove quote database");
    }

    #[test]
    fn upsert_overwrites_stale_values() {
        let path = test_db_path();
        let hash = [9_u8; 32];

        let db = QuoteDatabase::new(&path).expect("create quote database");
        db.insert_melt_payment_id(&hash, "0001-ln_old").unwrap();
        db.insert_melt_payment_id(&hash, "0002-ln_new").unwrap();

        assert_eq!(
            db.get_melt_payment_id(&hash).expect("get"),
            Some("0002-ln_new".to_string())
        );

        drop(db);
        std::fs::remove_file(path).expect("remove quote database");
    }

    #[test]
    fn concurrent_claims_only_allow_one_submission() {
        let dir = tempfile::tempdir().unwrap();
        let db = QuoteDatabase::new(dir.path().join("quotes.db")).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let db = db.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    db.begin_melt_attempt(&[1; 32], &QuoteId::new(), &CurrencyUnit::Sat)
                        .unwrap()
                })
            })
            .collect();
        let claimed = threads
            .into_iter()
            .map(|thread| usize::from(thread.join().unwrap()))
            .sum::<usize>();
        assert_eq!(claimed, 1);
    }

    #[test]
    fn rejection_requires_an_attempt_without_a_remote_index() {
        let dir = tempfile::tempdir().unwrap();
        let db = QuoteDatabase::new(dir.path().join("quotes.db")).unwrap();
        let hash = [1; 32];
        assert!(db.reject_melt_attempt(&hash).is_err());
        assert!(db.get_melt_attempt(&hash).unwrap().is_none());
        db.begin_melt_attempt(&hash, &QuoteId::new(), &CurrencyUnit::Sat)
            .unwrap();
        db.insert_melt_payment_id(&hash, "known remote index")
            .unwrap();
        assert!(db.reject_melt_attempt(&hash).is_err());
        assert!(
            !db.get_melt_attempt(&hash)
                .unwrap()
                .unwrap()
                .submission_rejected
        );
    }

    #[test]
    fn mint_quote_and_payment_id_written_in_single_call() {
        let path = test_db_path();
        let hash = [13_u8; 32];

        let db = QuoteDatabase::new(&path).expect("create quote database");
        db.insert_mint_quote_and_payment_id(&hash, "lnbc1incoming-invoice", "0001-ln_aabbcc")
            .expect("insert mint quote and payment index");

        assert_eq!(
            db.get_mint_quote(&hash).expect("get mint invoice"),
            Some("lnbc1incoming-invoice".to_string())
        );
        assert_eq!(
            db.get_mint_payment_id(&hash)
                .expect("get mint payment index"),
            Some("0001-ln_aabbcc".to_string())
        );

        drop(db);
        std::fs::remove_file(path).expect("remove quote database");
    }
}
