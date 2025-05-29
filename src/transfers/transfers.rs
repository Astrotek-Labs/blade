// external packages
use std::path::PathBuf;
use polars::prelude::*;
use anyhow::Result;

// internal code
use super::ingestion::TransferIngestion;
use crate::transfers::compression::{
    RLECompressedBlockNumberSeries, 
    RLECompressedTransactionIndexSeries, 
    RLECompressedLogIndexSeries,
    DictionaryCompressedTransactionHashSeries,
    RLECompressedErc20Series,
    NormalizedCompressedValueStrings,
    RLECompressedChainIdSeries
};
use super::writer::parquet_writer;

pub struct Transfer {
    pub dataframes: Vec<DataFrame>,            // vec of compressesd dataframes
    pub compressed_df: DataFrame,              // dataset after stacking 
    pub output_filepath: PathBuf,              // filepath for wrting compressed file
}

impl Transfer {

    pub fn new() -> Self {
        Self {
            dataframes: Vec::new(),                  // dataframe after schema check
            compressed_df: DataFrame::default(),     // compresed dataframe; pre file write
            output_filepath: PathBuf::new(),         // output filepath; 
        }
    }

    /// Set new filepath inplace with prefix "BLADE_". Store non-compressed data where you want output.
    pub fn _update_path(&mut self, filepath: &PathBuf) -> Result<()> {
        let mut path = PathBuf::from(filepath);
        let filename = path.file_name().unwrap().to_string_lossy();
        let amended_filename = format!("BLADE_{}", filename);
        path.set_file_name(amended_filename);
        self.output_filepath = path;
        Ok(())
    }


    pub fn write_parquet(&mut self, filepath: &PathBuf) -> Result<()> {

        // temporary write, check file creation
        self._update_path(filepath)?;
        println!("updating path at: {:?}", self.output_filepath);
        parquet_writer(self.output_filepath.clone(), self.dataframes.clone())?;
        
        if self.output_filepath.exists() {
            let size = std::fs::metadata(&self.output_filepath)?.len();
            println!("File created! Size: {} bytes", size);
        } else {
            println!("File was NOT created");
        }
        Ok(())
    }

    /// Compress iteratively goes through parquet file columns, applying specific
    /// compression algorithms to each, to maximize compression ratios.
    pub fn compress(&mut self, filepath: &PathBuf) -> Result<()> {
        
        // Instantiate TransferIngestion (ingestion.rs); validate schema against transfer dataset
        let mut transfer: TransferIngestion = TransferIngestion::new();
        let schema_check: DataFrame = transfer.check_schema_validity(filepath).unwrap();

        // let tcol = schema_check.column("transaction_hash");
        // println!("{:?}", tcol);

        // 1) block_number: rle compression
        let mut block_compression: RLECompressedBlockNumberSeries = RLECompressedBlockNumberSeries::new(); 
        let compressed_blocks_df = block_compression.create_compressed_df(&schema_check);
        self.dataframes.push(compressed_blocks_df?);

        // 2) transaction_index: rle compression
        let mut transaction_compression: RLECompressedTransactionIndexSeries = RLECompressedTransactionIndexSeries::new();
        let compressed_trans_index = transaction_compression.create_compressed_df(&schema_check);
        self.dataframes.push(compressed_trans_index?);

        // 3) log_index: rle compression
        let mut log_index_compression = RLECompressedLogIndexSeries::new();
        let compressed_log_index = log_index_compression.create_compressed_df(&schema_check);
        self.dataframes.push(compressed_log_index?);

        // 4) transaction_hash: dictionary encoding
        let mut transaction_hash_compression = DictionaryCompressedTransactionHashSeries::new();
        let compressed_transaction_df = transaction_hash_compression.create_compressed_df(&schema_check);
        self.dataframes.push(compressed_transaction_df?);

        // 5) erc20: rle compression
        let mut token_compression = RLECompressedErc20Series::new();
        let compressed_tokens_df = token_compression.create_compressed_df(&schema_check);
        self.dataframes.push(compressed_tokens_df?);
        // println!("compressed tokens: {:?}", compressed_tokens);



        // n) chain_id: rle compression
        let mut chain_id_compression = RLECompressedChainIdSeries::new();
        let compressed_chain_id_df = chain_id_compression.create_compressed_df(&schema_check);
        // println!("chain id compression: {:?}", compressed_chain_id_df?);
        self.dataframes.push(compressed_chain_id_df?);

        // 9) value_strings: normalization compression 
        let mut value_string_compression: NormalizedCompressedValueStrings = NormalizedCompressedValueStrings::new();
        let comopressed_value_string = value_string_compression.create_compressed_df(&schema_check);
        self.dataframes.push(comopressed_value_string?);


        // write to parquet
        self.write_parquet(filepath)?;

        Ok(())
    }


    // pub fn write_parquet(&mut self, filepath: &PathBuf) -> Result<()> {
    //     Ok(())
    // }
}
