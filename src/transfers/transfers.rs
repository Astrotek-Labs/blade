// use std::fs::File;
use std::path::PathBuf;
// use polars::prelude::*;

// internal code
use super::ingestion::TransferIngestion;
use crate::transfers::compression::{
    RLECompressedBlockNumberSeries, 
    RLECompressedTransactionIndexSeries, 
    NormalizedCompressedValueStrings
};

pub struct Transfer {
    // pub og_df: DataFrame,            // incoming dataset (from filepath)
    // pub compressed_df: DataFrame,    // dataset after compression
    pub output_filepath: PathBuf,       // filepath for wrting compressed file
}

impl Transfer {

    pub fn new() -> Self {
        Self {
            // og_df: DataFrame::default(),          // dataframe after schema check
            // compressed_df: DataFrame::default(),  // compresed dataframe; pre file write
            output_filepath: PathBuf::new(),         // output filepath; 
        }
    }

    /// Set new filepath based on incoming path. Will be same location, just with prefix "BLADE_"
    /// Best practice is to store input (non-compressed dataset) where you desire output.
    pub fn _update_path(&mut self, filepath: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        // set output file path; add 'BLADE_' designation
        let mut path = PathBuf::from(filepath);
        let filename = path.file_name().unwrap().to_string_lossy();
        // add designation to string lossy filename
        let amended_filename = format!("BLADE_{}", filename);
        // set file name, push to mut path
        path.set_file_name(amended_filename);
        self.output_filepath = path;
        Ok(())
    }

    /// Compress iteratively goes through parquet file columns, applying specific
    /// compression algorithms to each, to maximize compression ratios.
    pub fn compress(&mut self, filepath: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        
        // Instantiate TransferIngestion; validate schema against transfer dataset
        let mut transfer = TransferIngestion::new();
        let schema_check = transfer.check_schema_validity(filepath).unwrap();
        // println!("schema check is {}", schema_check);
        // let column_a = schema_check["value_string"].clone();
        // println!("{:?}", column_a);

        // NOTE: Ensure congruency in df creation for various column compressions
        /* Given there are multiple compression algorithms being used it is 
           imperative to use the proper df creator function via transfers/utils.rs.
           For example, RLE uses a value/count approach and will likely need 
           different approaches to construction. */
        
        // 1) block_number: rle compression
        let mut block_compression: RLECompressedBlockNumberSeries = RLECompressedBlockNumberSeries::new();
        let _compressed_blocks = block_compression.compress_block_number(&schema_check);
        // println!("Compressed blocks: {:?}", compressed_blocks);

        // 2) transaction_index: rle compression
        let mut transaction_compression: RLECompressedTransactionIndexSeries = RLECompressedTransactionIndexSeries::new();
        let _compressed_trans_index = transaction_compression.compress_transaction_index(&schema_check);
        // println!("Compressed transaction index: {:?}", compressed_trans_index);

        // n) value_strings: normalization compression 
        let mut value_string_compression: NormalizedCompressedValueStrings = NormalizedCompressedValueStrings::new();
        let _compressed_value_string = value_string_compression.compress_value_string(&schema_check);
        // let v_s_comp = compress_value_string(&schema_check);
        // println!("value string: {:?}", v_s_comp);


        let upfp = self._update_path(filepath);
        println!("output fp: {:?}", self.output_filepath);

        Ok(())
    }
}
