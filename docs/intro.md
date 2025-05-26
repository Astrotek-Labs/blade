# ORCHESTRATION PROCESS

1. Cross reference incoming dataset against valid schema
2. Pass columns through independent algorithms
   - Handled in separate files due to specialized compression algorithms for each column type
3. Generate dataframes, stack, prepare for writing to the aggregated parquet file
4. Write output parquet via `_update_path` impl
5. Output filepath is in place from incoming filepath with prefix of "_BLADE"




## Supported datasets
1. Transfers
2. Logs
3. Metadata (token)