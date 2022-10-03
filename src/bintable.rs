use super::tablerow::TableRow;
use std::fs::File;
use std::io::prelude::*;

pub struct BinTable {
    file: File,

    /// buffer_offset is used in case
    /// the current buffer had some remaining, incomplete bytes.
    /// In that case we'd like to clear the buffer in a way, that these bytes remain.
    /// Then append to these bytes.
    buffer_offset: usize,
    /// buffers some bytes into memory for parsing
    buffer: Vec<u8>,
    /// a buffer can hold information for 0 to n (n \in Nat) rows to parse.
    /// After parsing one row, we'd like to save a pointer to where in the parsing process we are inside the buffer.
    parsing_pointer: usize,
}

impl BinTable {
    pub fn open(path: &str) -> std::io::Result<BinTable> {
        let file = File::open(path)?;

        Ok(BinTable {
            file,
            buffer_offset: 0,
            buffer: vec![0u8; 1024],
            parsing_pointer: 0,
        })
    }
}

impl Iterator for BinTable {
    type Item = TableRow;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.len() == self.buffer_offset {
            self.buffer.extend((0..1024).map(|_| 0));
        }

        eprintln!("seek");
        // read to the end of the buffer
        let i = self
            .file
            .read(&mut self.buffer[self.buffer_offset..])
            .expect("read bintable file");

        // Advance the offset into the buffer,
        // so the next time we read, we won't override our data.
        self.buffer_offset += i;

        // can only mean there is nothign left to be read from the file.
        // if we have already consumed out buffer, we can end iteration here
        if i == 0 && self.buffer.len() == self. parsing_pointer{
            eprintln!("end of file");

            return None;
        }

        let tmpbuffer: &[u8] = &self.buffer[self.parsing_pointer..];

        if let Ok((row, rest)) = TableRow::from_bin(tmpbuffer) {
            // the buffer contained enough bytes to parse an entire row.
            let bytes_consumed = tmpbuffer.len() - rest.len();

            // advance the parsing pointer
            self.parsing_pointer += bytes_consumed;

            return Some(row);
        }

        // the buffer did not hold enough information to keep parsing!

        // more bytes need to be read!

        // we assume that the buffer is filled to the brim.

        //      Scenario A

        // we can't clear up space
        // the buffer isn't large enought to 
        if self.parsing_pointer == 0 {
            // No need to extend the buffer.
            // On the next invokation the buffer will get extended.
            return self.next();
        }

        
        //      Scenario B
        // clear up space in the buffer
        // by getting rid of the already parsed content.
        
        // stash unparsed bytes
        let fresh_bytes = tmpbuffer.to_vec();

        // adjust buffer offset so we write new content after the fresh bytes
        self.buffer_offset = fresh_bytes.len();
        // reset parsing pointer
        self.parsing_pointer = 0;

        // clear the buffer and write fresh_bytes back to it..
        self.buffer.clear();
        self.buffer.extend(fresh_bytes);
    
        // now seek from the file again
        self.next()
    }
}
