// TODO: implement writer function for stacked parquet dataframes


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
// let mut file = File::create("data/combined.parquet")?;
// ParquetWriter::new(&mut file).finish(&mut combined)?;
