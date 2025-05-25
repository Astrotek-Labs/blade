// use std::collections::HashMap;
use polars::prelude::*;


pub struct CreateDataFrame {
    pub rle_dataframe: DataFrame,
    pub vec_normalized_dataframe: DataFrame,
}


impl CreateDataFrame {

    pub fn new() -> Self {
        Self {
            rle_dataframe: DataFrame::default(),
            vec_normalized_dataframe: DataFrame::default(),
        }
    }


    /// RLE based dataframe creation
    // pub fn create_rle_dataframe() -> DataFrame {
    pub fn create_rle_dataframe() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }



    /// Vector Normalized dataframe creation
    pub fn create_vec_normalized_dataframe() -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

