 // Normalization compression for value_string column.
 // String --> BigDecimal --> f64 --> normalized vector.

use std::mem;
use anyhow::Result;
use std::str::FromStr;
use polars::prelude::*;
use owo_colors::OwoColorize;
use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use float_eq::assert_float_eq;

pub struct NormalizedCompressedValueStrings {
    index: Vec<u32>,
    normalized_vs_vec: Vec<f64>,
}

impl NormalizedCompressedValueStrings {

    pub fn new() -> Self {
        Self {
            index: Vec::new(),
            normalized_vs_vec: Vec::new(),
        }
    }

    /// Compress value string column of Transfer dataset through vector normalization.
    pub fn compress(&mut self, dataset: &DataFrame) -> Result<(Vec<u32>, Vec<f64>)> {

        // Distill value_string column from dataset and unwrap the str's
        let value_strings: &Column = dataset.column("value_string").unwrap();
        let value_strings_series = value_strings.str().unwrap();

        // Parse to BigDecimal, set scaled value for easier reference, push to vec
        let mut result: Vec<BigDecimal> = Vec::new();
        for val in value_strings_series.iter() {
            let decimal = BigDecimal::from_str(val.unwrap()).unwrap();
            // NOTE: check if scale of 10 is proper
            let divisor = BigDecimal::from(10);
            let scaled_value = decimal / divisor;
            result.push(scaled_value);
        }

        // Iterate through result, convert values to f64
        let result_f64: Vec<f64> = result.iter().map(|x: &BigDecimal| x.to_f64().unwrap_or(0.0)).collect();
        // Iterate; sum of squares
        let squared_sum: f64 = result_f64.iter().map(|x: &f64| x * x).sum();
        // Square root of sum of squares
        let sqrt_of_sum: f64 = squared_sum.sqrt();
        // Normalize vec 
        let normalized_vec: Vec<f64> = result_f64.iter().map(|x: &f64| x / sqrt_of_sum).collect();
        // Calculate sum of squares for assertion check
        let sum_of_squares: f64 = normalized_vec.iter().map(|x: &f64| x * x).sum();
        
        // NOTE: Optionally, assert equivalence between normalized vec and result vec
        // assert_eq!(normalized_vec.len(), result.len());

        // Assert that the sum of the squares is approximately 1.0
        // to verify normalization accuracy.
        assert_float_eq!(sum_of_squares, 1.0, abs <= 1e-10);

        // Calculate size of original string 
        let original_str_len = value_strings_series.iter()
            .map(|s| s.unwrap().len())
            .sum::<usize>();

        // Calculate size of compresed vec
        let compressed_size = normalized_vec.capacity() * mem::size_of::<u16>();
        let compression_ratio = original_str_len as f64 / compressed_size as f64;

        // // Print comparisons to terminal
        println!("[VALUE STRINGS MEM] {} → {} bytes ({:.2}x)", original_str_len.to_string().red(), compressed_size.to_string().green(), compression_ratio.to_string().bright_blue());

        // Get index len and set values to return
        self.normalized_vs_vec.extend(normalized_vec);
        self.index = (1..=self.normalized_vs_vec.len())
            .map(|i| i as u32)
            .collect();

        Ok((self.index.clone(), self.normalized_vs_vec.clone()))

    }

    
    pub fn create_compressed_df(&mut self, dataset: &DataFrame) -> Result<DataFrame> {
        // call compress function to create value / count references
        let _compressed_res = self.compress(dataset);
        let s1 = Column::new("value_string_index".into(), &self.index);
        let s2 = Column::new("value_string_normalized".into(), &self.normalized_vs_vec);
        let df = DataFrame::new(vec![s1, s2])?;
        Ok(df)
    }



    // TODO: decompress
//     pub fn decompress_value_string(&mut self, dataset: &DataFrame) -> Result<(), Box<dyn std::error::Error>> {
// 
//         Ok(())
//     }


}

