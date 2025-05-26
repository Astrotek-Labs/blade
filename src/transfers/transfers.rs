// external packages
use std::path::PathBuf;
use polars::prelude::*;
use anyhow::Result;

// internal code
use super::ingestion::TransferIngestion;
use crate::transfers::compression::{
    RLECompressedBlockNumberSeries, 
    RLECompressedTransactionIndexSeries, 
    NormalizedCompressedValueStrings
};

pub struct Transfer {
    // pub og_df: DataFrame,            // incoming dataset (from filepath)
    pub compressed_df: DataFrame,       // dataset after compression
    pub output_filepath: PathBuf,       // filepath for wrting compressed file
}

impl Transfer {

    pub fn new() -> Self {
        Self {
            // og_df: DataFrame::default(),          // dataframe after schema check
            compressed_df: DataFrame::default(),     // compresed dataframe; pre file write
            output_filepath: PathBuf::new(),         // output filepath; 
        }
    }

    /// Set new filepath inplace with prefix "BLADE_". Store non-compressed data where you want output.
    pub fn _update_path(&mut self, filepath: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let mut path = PathBuf::from(filepath);
        let filename = path.file_name().unwrap().to_string_lossy();
        let amended_filename = format!("BLADE_{}", filename);
        path.set_file_name(amended_filename);
        self.output_filepath = path;
        Ok(())
    }

    //  NOTE: Ensure congruency in df creation for various column compressions.
    /*  Given there are multiple compression algorithms being used it is 
        imperative to use the proper df creator function via transfers/utils.rs.
        For example, RLE uses a value/count approach and will likely need 
        different approaches to construction. */

    /// Compress iteratively goes through parquet file columns, applying specific
    /// compression algorithms to each, to maximize compression ratios.
    pub fn compress(&mut self, filepath: &PathBuf) -> Result<()> {
        
        // Instantiate TransferIngestion (ingestion.rs); validate schema against transfer dataset
        let mut transfer: TransferIngestion = TransferIngestion::new();
        let schema_check: DataFrame = transfer.check_schema_validity(filepath).unwrap();

        // OPTIONAL: check for size of schema col vs compressed col
        // let series = schema_check.column("block_number")?;
        // let temp_df = DataFrame::new(vec![series.clone()])?;
        // let series_memory = temp_df.estimated_size();
        // println!("est sch size {:?}", series_memory);


       
        // 1) block_number: rle compression
        let mut block_compression: RLECompressedBlockNumberSeries = RLECompressedBlockNumberSeries::new(); 
        // let _compressed_blocks: Result<(Vec<u32>, Vec<u32>), anyhow::Error> = block_compression.compress_block_number(&schema_check);
        let compressed_blocks_df = block_compression.create_compressed_df(&schema_check);
        println!("{:?}", compressed_blocks_df);

//         // 2) transaction_index: rle compression
//         let mut transaction_compression: RLECompressedTransactionIndexSeries = RLECompressedTransactionIndexSeries::new();
//         let _compressed_trans_index = transaction_compression.compress_transaction_index(&schema_check);
// 
//         // n) value_strings: normalization compression 
//         let mut value_string_compression: NormalizedCompressedValueStrings = NormalizedCompressedValueStrings::new();
//         let _compressed_value_string = value_string_compression.compress_value_string(&schema_check);

        Ok(())
    }


    // pub fn write_parquet(&mut self, filepath: &PathBuf) -> Result<()> {
    //     Ok(())
    // }
}
