//! Database module for storing quote-to-payment mappings
//!
//! Uses redb to store mappings between mint/melt quotes and Spark payment IDs

use anyhow::Result;
use redb::{Database, ReadableTable, TableDefinition, ReadableDatabase};
use std::path::Path;
use std::sync::Arc;

/// Table for storing mint quote ID to Spark payment ID mappings
const MINT_QUOTES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("mint_quotes");

/// Table for storing melt quote ID to Spark payment ID mappings
const MELT_QUOTES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("melt_quotes");

/// Database wrapper for quote-to-payment mappings
#[derive(Clone)]
pub struct QuoteDatabase {
    db: Arc<Database>,
}

impl QuoteDatabase {
    /// Create a new database instance or open an existing one
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;

        // Create tables if they don't exist
        let write_txn = db.begin_write()?;
        {
            let _mint_table = write_txn.open_table(MINT_QUOTES_TABLE)?;
            let _melt_table = write_txn.open_table(MELT_QUOTES_TABLE)?;
        }
        write_txn.commit()?;

        tracing::info!("Database initialized with mint_quotes and melt_quotes tables");

        Ok(Self {
            db: Arc::new(db),
        })
    }

    /// Store a mint quote ID to Spark payment ID mapping
    pub fn insert_mint_quote(&self, quote_id: &str, payment_id: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MINT_QUOTES_TABLE)?;
            table.insert(quote_id, payment_id)?;
        }
        write_txn.commit()?;
        tracing::debug!("Inserted mint quote mapping: {} -> {}", quote_id, payment_id);
        Ok(())
    }

    /// Store a melt quote ID to Spark payment ID mapping
    pub fn insert_melt_quote(&self, quote_id: &str, payment_id: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MELT_QUOTES_TABLE)?;
            table.insert(quote_id, payment_id)?;
        }
        write_txn.commit()?;
        tracing::debug!("Inserted melt quote mapping: {} -> {}", quote_id, payment_id);
        Ok(())
    }

    /// Get the Spark payment ID for a mint quote
    pub fn get_mint_quote(&self, quote_id: &str) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MINT_QUOTES_TABLE)?;

        let result = table.get(quote_id)?;
        Ok(result.map(|v| v.value().to_string()))
    }

    /// Get the Spark payment ID for a melt quote
    pub fn get_melt_quote(&self, quote_id: &str) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MELT_QUOTES_TABLE)?;

        let result = table.get(quote_id)?;
        Ok(result.map(|v| v.value().to_string()))
    }

    /// Delete a mint quote mapping
    pub fn delete_mint_quote(&self, quote_id: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MINT_QUOTES_TABLE)?;
            table.remove(quote_id)?;
        }
        write_txn.commit()?;
        tracing::debug!("Deleted mint quote mapping: {}", quote_id);
        Ok(())
    }

    /// Delete a melt quote mapping
    pub fn delete_melt_quote(&self, quote_id: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(MELT_QUOTES_TABLE)?;
            table.remove(quote_id)?;
        }
        write_txn.commit()?;
        tracing::debug!("Deleted melt quote mapping: {}", quote_id);
        Ok(())
    }

    /// List all mint quote mappings
    pub fn list_mint_quotes(&self) -> Result<Vec<(String, String)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MINT_QUOTES_TABLE)?;

        let mut results = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            results.push((key.value().to_string(), value.value().to_string()));
        }

        Ok(results)
    }

    /// List all melt quote mappings
    pub fn list_melt_quotes(&self) -> Result<Vec<(String, String)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MELT_QUOTES_TABLE)?;

        let mut results = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            results.push((key.value().to_string(), value.value().to_string()));
        }

        Ok(results)
    }
}
