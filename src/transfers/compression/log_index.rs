use std::mem;
use anyhow::Result;
use polars::prelude::*;
use owo_colors::OwoColorize;

pub struct RLECompressedLogIndexSeries {
    pub values: Vec<u32>,    // Unique values in sequence
    pub counts: Vec<u32>,    // Count of consecutive repetitions
}

impl RLECompressedLogIndexSeries {

    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            counts: Vec::new(),
        }
    }

    /// Compress log index number column in transfers dataset through RLE methodology.
    pub fn compress(&mut self, dataset: &DataFrame) -> Result<(Vec<u32>, Vec<u32>)> {

        // Distill log_index column from incoming dataset and convert to u32
        let log_index = dataset.column("log_index").unwrap();
        let log_index_vec: Vec<Option<u32>> = log_index.u32()?.into_iter().collect();

        // Return empty tuple of vec if empty
        if log_index_vec.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // Iterate with the first value of vector; set count to 1.
        let mut current_value: u32  = log_index_vec[0].unwrap();
        let mut current_count: u32 = 1;

        // Iterate through log_index_vec, skipping the first
        // since that is set as current_value
        for log in log_index_vec.iter().skip(1) {
            let b = log.unwrap();
            if b == current_value {
                current_count += 1 as u32;
            } else {
                self.values.push(current_value);
                self.counts.push(current_count.into());

                current_value = b;
                current_count = 1;
            }
        }
        self.values.push(current_value);
        self.counts.push(current_count.into());

        // Check size comparisons
        let log_index_size = log_index_vec.capacity() * mem::size_of::<Option<u32>>();
        let compressed_size = self.values.capacity() * mem::size_of::<u32>() + 
                            self.counts.capacity() * mem::size_of::<u16>();
        let compression_ratio = log_index_size as f64 / compressed_size as f64;

        // Optional output print statements for comparison
        println!("Original log index: {} bytes", log_index_size.red());
        println!("Compressed log index: {} bytes", compressed_size.green());
        println!("Compression ratio {:.2}", compression_ratio.bright_blue());

        // assert that output is equal in len to input
        // assert_eq!()
        Ok((self.values.clone(), self.counts.clone()))

    }


    pub fn create_compressed_df(&mut self, dataset: &DataFrame) -> Result<DataFrame> {
        // call compress function to create value / count references
        let _compressed_res = self.compress(dataset);
        let s1 = Column::new("log_index_values".into(), &self.values);
        let s2 = Column::new("log_index_counts".into(), &self.counts);
        let df = DataFrame::new(vec![s1, s2])?;
        Ok(df)
    }

    /// Decompression of RLE compressed block number data in the transfer dataset.
    pub fn decompress() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}



