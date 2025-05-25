use std::mem;
use polars::prelude::*;
use owo_colors::OwoColorize;

pub struct RLECompressedTransactionIndexSeries {
    pub values: Vec<u32>,    // Unique values in sequence
    pub counts: Vec<u16>,    // Count of consecutive repetitions
}

impl RLECompressedTransactionIndexSeries {

    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            counts: Vec::new(),
        }
    }

    pub fn compress_transaction_index(&mut self, dataset: &DataFrame) -> Result<(Vec<u32>, Vec<u16>), Box<dyn std::error::Error>> {
        // establish incoming col len // let num_rows = dataset.height();

        let transaction_index = dataset.column("transaction_index").unwrap();
        let transaction_index_vec: Vec<Option<u32>> = transaction_index.u32()?.into_iter().collect();
        // println!("{:?}", transaction_index_vec);

        // early return if vec is empty
        if transaction_index_vec.is_empty() {
            return Ok((Vec::new(), Vec::new()))
        }

       // set initial transaction index as current value, and initial count as 1
        let mut current_value = transaction_index_vec[0].unwrap();
        let mut current_count = 1 as u16;

        // iterate through transaction index, skip first, 
        for transaction_index in transaction_index_vec.iter().skip(1) {
            let b = transaction_index.unwrap();
            if b == current_value {
                current_count += 1 as u16;
            } else {
                self.values.push(current_value);
                self.counts.push(current_count.into());

                current_value = b;
                current_count = 1;
            }
        }
        self.values.push(current_value);
        self.counts.push(current_count.into());

        // check size comparisons
        let transaction_index_size = transaction_index_vec.capacity() * mem::size_of::<Option<u32>>();
        let compressed_size = self.values.capacity() * mem::size_of::<u32>() + 
                            self.counts.capacity() * mem::size_of::<u16>();

        println!("Original transaction index: {} bytes", transaction_index_size.red());
        println!("Compressed transaction index: {} bytes", compressed_size.green());
        println!("Compression Ratio: {:.2}", transaction_index_size as f64 / compressed_size as f64);

        // assert that output is equal in len to input
        // assert_eq!()

        Ok((self.values.clone(), self.counts.clone()))
    }
}



