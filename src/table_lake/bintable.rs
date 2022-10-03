use super::tablerow::TableRow;
use std::fs::File;
use std::io::prelude::*;
use std::sync::mpsc::Sender;

use anyhow::Result;

use super::TableLakeReader;

pub struct BinTable {
    file: File,
    limit: usize,
}

impl BinTable {
    pub fn open(path: &str, limit: usize) -> Result<BinTable> {
        let file = File::open(path)?;

        Ok(BinTable { file, limit })
    }
}

impl TableLakeReader for BinTable {
    fn read(&mut self, ch: Sender<super::Entry>) {
        if self.limit == 0 {
            return;
        }

        let mut start = 0;
        let mut buffer = vec![0u8; 1024];
        // let mut buffer = Vec::with_capacity(1024);

        loop {
            if buffer.len() == start {
                buffer.extend(0..255)
            }
            eprintln!("seek");
            // read to the end of the buffer
            let i = self
                .file
                .read(&mut buffer[start..])
                .expect("read bintable file");

            if i == 0 {
                eprintln!("end of file");
                return;
            }

            let mut tmpbuffer: &mut [u8] = &mut buffer;
            let mut once = false;
            while let Ok((row, rest)) = TableRow::from_bin(tmpbuffer) {
                once = true;
                ch.send(row.into_entry()).expect("stream rows to channel");

                self.limit -= 1;
                if self.limit == 0 {
                    return;
                }

                let bytes_consumed = tmpbuffer.len() - rest.len();

                // advance buffer
                tmpbuffer = &mut tmpbuffer[bytes_consumed..]
            }

            if !once {
                eprintln!("reserving more size for buffer");
                buffer.extend(0..255);
                start += 255;
                continue;
            }

            // no complete row can be read now.
            // but the buffer needs to respect that some partial row is still loaded already.
            let remaining_bytes = tmpbuffer.to_vec();
            start = remaining_bytes.len();

            // remaining bytes contain partial row
            buffer.clear();
            // write back remaining row to buffer
            buffer.extend(remaining_bytes);
        }

        // designate this thread to decoding
    }
}
