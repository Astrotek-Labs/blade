use std::mem;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use owo_colors::OwoColorize;

pub struct RLECompressedLogIndexSeries {
    pub unique_hashes: HashMap   // Unique transaction hashes
}

impl RLECompressedLogIndexSeries {

    pub fn new() -> Self {
        Self {
            unique_hashes: HashMap::new(),
        }
    }


    pub fn compress(&mut self, dataset: &DataFrame) -> Result<()> {
        Ok(())
    }


}

