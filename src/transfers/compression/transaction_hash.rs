// use std::mem;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use owo_colors::OwoColorize;

pub struct DictionaryCompressedTransactionHashSeries {
    pub unique_hashes: HashMap<u32, String>   // Unique transaction hashes
}

impl DictionaryCompressedTransactionHashSeries {

    pub fn new() -> Self {
        Self {
            unique_hashes: HashMap::new(),
        }
    }

    pub fn compress(&mut self, dataset: &DataFrame) -> Result<()> {
        let ca: ChunkedArray<BooleanType> = dataset.is_unique()?;
        // println!("unique vals: {:?}", ca);

        Ok(())
    }


}

