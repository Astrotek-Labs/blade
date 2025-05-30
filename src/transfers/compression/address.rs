use hex::{decode, encode};
use std::mem;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use owo_colors::OwoColorize;
use std::collections::HashSet;


pub struct DictionaryCompressedAddressSeries {
    pub index: Vec<u32>,
    pub address_pairs: Vec<String>,
}

pub enum CompressResult {
    Compressed(Vec<u32>, Vec<String>),
    // Original(Column, Column),
    Original(DataFrame),
}

impl DictionaryCompressedAddressSeries {

    pub fn new() -> Self {
        Self {
            index: Vec::new(),
            address_pairs: Vec::new(),
        }
    }

    pub fn compress(&mut self, dataset: &DataFrame) -> Result<CompressResult> {
        let from_addresses: &Column = dataset.column("from_address").unwrap();
        let to_addresses: &Column = dataset.column("to_address").unwrap();

        // Check uniqueness ratio
        let unique_from = from_addresses.n_unique()?;
        let unique_to = to_addresses.n_unique()?;
        let total_rows = dataset.height();
        
        let from_ratio = unique_from as f64 / total_rows as f64;
        let to_ratio = unique_to as f64 / total_rows as f64;

        if from_ratio > 0.3 || to_ratio > 0.3 {
            // Before cloning, deduplicate strings
            // Convert to categorical (dictionary-encoded)

            let from_cat = from_addresses.cast(&DataType::String)?;
            let to_cat = to_addresses.cast(&DataType::String)?;
            let df = DataFrame::new(vec![from_cat, to_cat])?;
            return Ok(CompressResult::Original(df));
        }

        // Compression logic
        let from_address_series = from_addresses.str().unwrap();
        let to_address_series = to_addresses.str().unwrap();
        let mut address_to_index: HashMap<String, u32> = HashMap::new();
        let mut unique_pairs: Vec<String> = Vec::new();
        let mut all_indices: Vec<u32> = Vec::new();

        for (from, to) in from_address_series.iter().zip(to_address_series.iter()) {
            let combined = format!("{}{}", &from.unwrap()[2..], &to.unwrap()[2..]);
            
            let dict_index = if let Some(&idx) = address_to_index.get(&combined) {
                idx
            } else {
                let new_idx = unique_pairs.len() as u32;
                address_to_index.insert(combined.clone(), new_idx);
                unique_pairs.push(combined);
                new_idx
            };
            all_indices.push(dict_index);
        }

        self.index = all_indices.clone();
        self.address_pairs = unique_pairs.clone();

        Ok(CompressResult::Compressed(all_indices, unique_pairs))
    }


    pub fn create_compressed_df(&mut self, dataset: &DataFrame) -> Result<Vec<DataFrame>> {
        match self.compress(dataset)? {
            CompressResult::Compressed(_, _) => {
                // Use self.index and self.address_pairs
                let mut final_columns = Vec::new();
                let s1 = Column::new("address_index".into(), &self.index);
                let df1 = DataFrame::new(vec![s1]);
                let s2 = Column::new("address_values".into(), &self.address_pairs);
                let df2 = DataFrame::new(vec![s2]);
                final_columns.push(df1?);
                final_columns.push(df2?);
                Ok(final_columns)
            },
            CompressResult::Original(df) => {
                Ok(vec![df])
            }
        }
    }

    // pub fn decompress(&mut self) -> Result<()> {
    //     // decode 
    //     let reconstructed = format!("0x{}", hex::encode(&hash_bytes));
    //     Ok(())
    // }


}

