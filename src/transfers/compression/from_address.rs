use hex::{decode, encode};
use std::mem;
use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use owo_colors::OwoColorize;
use std::collections::HashSet;


pub struct DictionaryCompressedFromAddressSeries {
    pub index: Vec<u32>,
    // pub from_addresses: Vec<Vec<u8>>,
    pub from_addresses: Vec<String>,
}

impl DictionaryCompressedFromAddressSeries {

    pub fn new() -> Self {
        Self {
            index: Vec::new(),
            from_addresses: Vec::new(),
        }
    }

    pub fn compress(&mut self, dataset: &DataFrame) -> Result<(Vec<u32>, Vec<String>)> {

        // extracted column data
        let addresses = dataset.column("from_address").unwrap();

        // data for conditional logic
        let unique_addresses: usize = dataset.column("from_address").unwrap().n_unique()?;
        let total_addresses: usize = addresses.len();
        let unique_ratio: f64 = unique_addresses as f64 / total_addresses as f64;
        println!("from address unique ratio: {:?}", unique_ratio);

        let from_address_series = addresses.str().unwrap();

        if unique_ratio < 0.3 {
            let mut address_to_index: HashMap<String, u32> = HashMap::new();
            let mut unique_addresses: Vec<String> = Vec::new();
            let mut all_indices: Vec<u32> = Vec::new();
            
            for item in from_address_series.iter() {
                let val = item.unwrap();
                let clean_addr = val[2..].to_string();
                
                let dict_index = if let Some(&idx) = address_to_index.get(&clean_addr) {
                    idx
                } else {
                    let new_idx = unique_addresses.len() as u32;
                    address_to_index.insert(clean_addr.clone(), new_idx);
                    unique_addresses.push(clean_addr);
                    new_idx
                };
                all_indices.push(dict_index);
            }
            
            self.index = all_indices;
            self.from_addresses = unique_addresses;

        } else {
            // remove '0x' and aggregate
            println!("unique ratio > 0.3");
            for (index, item) in from_address_series.iter().enumerate() {
                let val = item.unwrap();
                let formatted_val = &val[2..].to_string();
                // let hex_string = hex::decode(formatted_val).unwrap();
                self.index.push(index as u32);
                self.from_addresses.push(formatted_val.clone());
            }

        }
        
        // Output stats to terminal
        let uncompressed_mem_size = addresses.len() * std::mem::size_of::<polars::datatypes::AnyValue>();
        let compressed_size = self.index.capacity() * mem::size_of::<u32>() + 
                            self.from_addresses.capacity() * mem::size_of::<u16>();
        let compression_ratio = uncompressed_mem_size as f64 / compressed_size as f64;
        println!("[FROM ADDRESS MEM] {} → {} bytes ({:.2}x)", uncompressed_mem_size.to_string().red(), compressed_size.to_string().green(), compression_ratio.to_string().bright_blue());

        Ok((self.index.clone(), self.from_addresses.clone()))
    }


    pub fn create_compressed_df(&mut self, dataset: &DataFrame) -> Result<Vec<DataFrame>> {
        // call compress function to create value / count references
        let _compressed_res = self.compress(dataset);
        let mut final_columns = Vec::new();

        let s1 = Column::new("from_address_index".into(), &self.index);
        let df1 = DataFrame::new(vec![s1]);

        let s2 = Column::new("from_address_values".into(), &self.from_addresses);
        let df2 = DataFrame::new(vec![s2]);

        final_columns.push(df1?);
        final_columns.push(df2?);

        Ok(final_columns)
    }

    // pub fn decompress(&mut self) -> Result<()> {
    //     // decode 
    //     let reconstructed = format!("0x{}", hex::encode(&hash_bytes));
    //     Ok(())
    // }


}

