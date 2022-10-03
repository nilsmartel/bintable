mod bintable;
mod tablerow;
pub use bintable::BinTable;
use get_size::GetSize;

use std::sync::mpsc::Sender;

#[derive(GetSize, Clone, Copy, Debug)]
pub struct TableLocation {
    pub tableid: u32,
    pub colid: u32,
    pub rowid: u64,
}

impl TableLocation {
    pub fn new(tableid: u32, colid: u32, rowid: u64) -> Self {
        Self {
            tableid,
            rowid,
            colid,
        }
    }

    pub fn integers(self) -> [u32; 3] {
        let TableLocation {
            tableid,
            colid,
            rowid,
        } = self;

        let rowid = if rowid <= std::u32::MAX as u64 {
            rowid as u32
        } else {
            println!(
                "error in TableIndex::integers, row index (TableLocation::rowid) is to high {}",
                rowid
            );
            rowid.min(std::u32::MAX as u64) as u32
        };

        [tableid, colid, rowid]
    }
}

pub type Entry = (String, TableLocation);

/// Trait used to digest multiple tables
/// from various sources.
pub trait TableLakeReader
where
    Self: Send,
{
    fn read(&mut self, ch: Sender<Entry>);
}
