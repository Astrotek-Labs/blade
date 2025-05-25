use std::path::PathBuf;
use polars::prelude::*;

pub struct TransferIngestion {
    pub reference_df: DataFrame,
}

impl TransferIngestion {

    pub fn new() -> Self {
        Self {
            reference_df: DataFrame::default(),
        }
    }

    /// Generate default `transfer` schema to reference input validity
    pub fn _generate_reference(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let df: DataFrame = df!(
            "block_number" => [0_u32],
            "transaction_index" => [0_u32],
            "log_index" => [0_u32],
            "transaction_hash" => [""],
            "erc20" => [""],
            "from_address" => [""],
            "to_address" => [""],
            "value_binary" => [""],
            "value_string" => [""],
            "value_f64" => [0.0],
            "chain_id" => [0_u64],
        )
        .unwrap();
        self.reference_df = df;
        Ok(())
    }

    /// Check input validity of parquet file against default schema
    pub fn check_schema_validity(&mut self, filepath: &PathBuf) -> PolarsResult<DataFrame> {
        // Generate reference dataframe to check incoming schema
        let _ = self._generate_reference();
        
        // Open file path and attempt to turn into dataframe
        let mut file = std::fs::File::open(filepath).unwrap();
        let df = ParquetReader::new(&mut file).finish()?;

        // Get schema from incoming dataset and reference schema
        let sch = df.schema();
        let ref_sch = self.reference_df.schema();

        // Assert that schemas match
        assert_eq!(sch, ref_sch, "Schema mismatch for file {:?}: expected reference schema", filepath);
        Ok(df)
    } 

}


