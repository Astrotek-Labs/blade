use hex::{decode, encode};
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use owo_colors::OwoColorize;
use std::collections::HashSet;


pub struct DictionaryCompressedTransactionHashSeries {
    // pub unique_hashes: HashMap<u32, Vec<u8>>,   // Unique transaction hashes
    pub index: Vec<u32>,
    pub hashes: Vec<Vec<u8>>,
    // pub hash_dataframe: DataFrame,
}

impl DictionaryCompressedTransactionHashSeries {

    pub fn new() -> Self {
        Self {
            // unique_hashes: HashMap::new(),
            index: Vec::new(),
            hashes: Vec::new(),
            // hash_dataframe: DataFrame::default(),
        }
    }

    pub fn compress(&mut self, dataset: &DataFrame) -> Result<()> {

        let tx_hashes = dataset.column("transaction_hash").unwrap();
        let uncompressed_mem_size = tx_hashes.len() * std::mem::size_of::<polars::datatypes::AnyValue>();
        println!("uncompressed tx hash size: {:?}", uncompressed_mem_size);

        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let tx_hash_series = tx_hashes.str().unwrap();

        for (index, item) in tx_hash_series.iter().enumerate() {
            let val = item.unwrap();
            let formatted_val = &val[2..];
            let hex_string = hex::decode(formatted_val).unwrap();

            if seen.insert(hex_string.clone()) {
                self.index.push(index as u32);
                self.hashes.push(hex_string);
            }
        }
        // println!("index: {:?}", self.index);
        // println!("hash: {:?}", self.hashes);
        Ok(())
    }


    pub fn create_compressed_df(&mut self, dataset: &DataFrame) -> Result<DataFrame> {
        // call compress function to create value / count references
        let _compressed_res = self.compress(dataset);
        let s1 = Column::new("tx_hash_index".into(), &self.index);
        let s2 = Column::new("tx_hash_values".into(), &self.hashes);
        let df = DataFrame::new(vec![s1, s2])?;

        let compressed_mem_size: usize = df.get_columns()
            .iter()
            .map(|col| col.len() * std::mem::size_of::<polars::datatypes::AnyValue>())
            .sum();
        println!("compressed tx hash size: {:?}", compressed_mem_size);
        Ok(df)
    }



    // pub fn decompress(&mut self) -> Result<()> {
    //     // decode 
    //     let reconstructed = format!("0x{}", hex::encode(&hash_bytes));
    //     Ok(())
    // }


}

