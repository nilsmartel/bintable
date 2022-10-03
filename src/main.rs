mod table_lake;
use std::{sync::mpsc::channel, thread::spawn};

use table_lake::TableLakeReader;

fn main() {
    let bintablefile = std::env::args()
        .nth(1)
        .expect("find bintablefile as first argument");

    let mut table = table_lake::BinTable::open(&bintablefile, 1000).unwrap();

    let (s, r) = channel();
    eprintln!("start reading");
    let handle = spawn(move || table.read(s));

    let mut i = 0;
    for (s, row) in r {
        eprint!("[{:02}] ", i);
        println!("'{s}': {:?}", row);
        i += 1;
    }

    handle.join().expect("join thread");
}
