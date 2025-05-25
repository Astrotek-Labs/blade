use std::env;
use std::path::PathBuf;
// use polars::prelude::*;

// use proton::transfers::*;
use proton::transfers::transfers::Transfer;

fn main() {
    // get args 
    let args: Vec<String> = env::args().collect();

    // instantiate transfer
    let mut transfers = Transfer::new();
    let _o = transfers.compress(&PathBuf::from(&args[1])).unwrap();


}
