// TODO: Implement writer function for stacked parquet dataframes.
//       Current thought is to stack individual columns as unique
//       dataframes, and then append into one parquet file to write.


// example code for writing stacked parquet dfs
// TODO: solve for index values
// let mut df1 = df!(
//     "id" => &[1, 2, 3],
//     "name" => &["a", "b", "c"]
// )?;

// // Second dataframe
// let df2 = df!(
//     "id" => &[4, 5, 6, 7],
//     "name" => &["d", "e", "f", "g"]
// )?;

// // Concatenate
// let mut combined = df1.vstack(&df2)?;

// // Write to Parquet
// TODO: write to output file path corresponding to data type (eg: Transfers)
// let mut file = File::create("data/combined.parquet")?;
// ParquetWriter::new(&mut file).finish(&mut combined)?;


use std::fs::File;
use anyhow::Result;
use std::path::PathBuf;
use polars::prelude::*;

pub fn parquet_writer(output_filepath: PathBuf, dataframes: Vec<DataFrame>) -> Result<()> {

    // Stack all DataFrames vertically
    let mut combined_df = dataframes[0].clone();
    for df in &dataframes[1..] {
        combined_df = combined_df.vstack(df)?;
    }

    // Write to parquet
    // let mut file = File::create("data/combined.parquet")?;

    let mut file = File::create(&output_filepath)?;
    println!("file {:?}", file);
    ParquetWriter::new(&mut file).finish(&mut combined_df)?;

    Ok(())
}