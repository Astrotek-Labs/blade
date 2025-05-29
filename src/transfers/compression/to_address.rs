use hex::{decode, encode};
use std::mem;
use anyhow::Result;
use polars::prelude::*;
// use std::collections::HashMap;
use owo_colors::OwoColorize;
use std::collections::HashSet;


pub struct DictionaryCompressedToAddressSeries {
    pub index: Vec<u32>,
    pub to_addresses: Vec<Vec<u8>>,
}

impl DictionaryCompressedToAddressSeries {

    pub fn new() -> Self {
        Self {
            index: Vec::new(),
            to_addresses: Vec::new(),
        }
    }

    pub fn compress(&mut self, dataset: &DataFrame) -> Result<()> {

        let addresses = dataset.column("to_address").unwrap();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let to_address_series = addresses.str().unwrap();

        for (index, item) in to_address_series.iter().enumerate() {
            let val = item.unwrap();
            let formatted_val = &val[2..];
            let hex_string = hex::decode(formatted_val).unwrap();
            if seen.insert(hex_string.clone()) {
                self.index.push(index as u32);
                self.to_addresses.push(hex_string);
            }
        }

        // Output stats to terminal
        let uncompressed_mem_size = addresses.len() * std::mem::size_of::<polars::datatypes::AnyValue>();
        let compressed_size = self.index.capacity() * mem::size_of::<u32>() + 
                            self.to_addresses.capacity() * mem::size_of::<u16>();
        let compression_ratio = uncompressed_mem_size as f64 / compressed_size as f64;
        println!("[TO ADDRESS MEM] {} → {} bytes ({:.2}x)", uncompressed_mem_size.to_string().red(), compressed_size.to_string().green(), compression_ratio.to_string().bright_blue());

        Ok(())
    }


    pub fn create_compressed_df(&mut self, dataset: &DataFrame) -> Result<DataFrame> {
        // call compress function to create value / count references
        let _compressed_res = self.compress(dataset);
        let s1 = Column::new("to_address_index".into(), &self.index);
        let s2 = Column::new("to_address_values".into(), &self.to_addresses);
        let df = DataFrame::new(vec![s1, s2])?;

        let compressed_mem_size: usize = df.get_columns()
            .iter()
            .map(|col| col.len() * std::mem::size_of::<polars::datatypes::AnyValue>())
            .sum();
        println!("compressed to address size: {:?}", compressed_mem_size);
        Ok(df)
    }

    // pub fn decompress(&mut self) -> Result<()> {
    //     // decode 
    //     let reconstructed = format!("0x{}", hex::encode(&hash_bytes));
    //     Ok(())
    // }


}

