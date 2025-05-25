//! Proton: A lossless compression library for blockchain parquet data. 
//! 
//! This crate provides functionality for handling blockchain transfers,
//! logs, and metadata. 

//! Transfer sequence:
//! Drives through transfers.rs
//! 1) Verify incoming schema is equal to reference value (ingestion.rs)
//! 2) Pass through compression algos (compression.rs)
//! 3) Write to new file (writer.rs)

// mods
pub mod transfers;
