use std::mem;
use anyhow::Result;
use polars::prelude::*;
use owo_colors::OwoColorize;

pub struct RLECompressedErc20Series {
    pub values: Vec<String>,    // Unique values in sequence
    pub counts: Vec<u32>,       // Count of consecutive repetitions
}

impl RLECompressedErc20Series {

    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            counts: Vec::new(),
        }
    }

    /// Compress erc20 column in transfers dataset through RLE methodology.
    pub fn compress(&mut self, dataset: &DataFrame) -> Result<(Vec<String>, Vec<u32>)> {

        // Distill erc20 column from incoming dataset and convert to string
        let tokens = dataset.column("erc20").unwrap();
        let token_strings_series: Vec<_> = tokens.str()?.into_iter().collect();

        // Return empty tuple of vec if empty
        if token_strings_series.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // Iterate with the first value of vector; set count to 1.
        let mut current_value: &str = token_strings_series[0].unwrap();
        let mut current_count: u32 = 1;

        // Iterate through token_string_series, skipping the first
        // since that is set as current_value
        for token in token_strings_series.iter().skip(1) {
            let b = token.unwrap();
            if b == current_value {
                current_count += 1 as u32;
            } else {
                self.values.push(current_value.to_string());
                self.counts.push(current_count.into());

                current_value = b;
                current_count = 1;
            }
        }
        self.values.push(current_value.to_string());
        self.counts.push(current_count.into());

        // Check size comparisons
        let token_size = token_strings_series.capacity() * mem::size_of::<Option<u32>>();
        let compressed_size = self.values.capacity() * mem::size_of::<u32>() + 
                            self.counts.capacity() * mem::size_of::<u16>();
        let compression_ratio = token_size as f64 / compressed_size as f64;

        // // Optional output print statements for comparison
        println!("[ERC20 MEM] {} → {} bytes ({:.2}x)", token_size.to_string().red(), compressed_size.to_string().green(), compression_ratio.to_string().bright_blue());

        // assert that output is equal in len to input
        // assert_eq!()
        Ok((self.values.clone(), self.counts.clone()))

    }


    pub fn create_compressed_df(&mut self, dataset: &DataFrame) -> Result<DataFrame> {
        // call compress function to create value / count references
        let _compressed_res = self.compress(dataset);
        let s1 = Column::new("token_values".into(), &self.values);
        let s2 = Column::new("token_counts".into(), &self.counts);
        let df = DataFrame::new(vec![s1, s2])?;
        Ok(df)
    }

    /// Decompression of RLE compressed block number data in the transfer dataset.
    pub fn decompress() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}



