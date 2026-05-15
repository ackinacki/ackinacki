//! Print archive state store summary.
//!
//! Usage: archive-summary <aerospike-address>
//!
//! Example: archive-summary 127.0.0.1:4000

use account_state::ArchiveStateStore;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <aerospike-address>", args[0]);
        eprintln!("Example: {} 127.0.0.1:4000", args[0]);
        std::process::exit(1);
    }

    let address = &args[1];

    match ArchiveStateStore::connect_aerospike(address) {
        Ok(store) => println!("{}", store.summary()),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}
