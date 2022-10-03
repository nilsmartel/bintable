use anyhow::Result;
use fast_smaz::Smaz;
use postgres::Row;
use varint_compression::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub tokenized: String,
    pub tableid: u32,
    pub colid: u32,
    pub rowid: u64,
}

fn get_number(row: &Row, idx: &str) -> i64 {
    if let Ok(n) = row.try_get::<_, i32>(idx) {
        return n as i64;
    }

    if let Ok(n) = row.try_get::<_, i64>(idx) {
        return n as i64;
    }

    if let Ok(n) = row.try_get::<_, i8>(idx) {
        return n as i64;
    }

    if let Ok(n) = row.try_get::<_, i16>(idx) {
        return n as i64;
    }

    // We error here on purpose.
    row.get(idx)
}

impl From<&Row> for TableRow {
    fn from(row: &Row) -> Self {
        TableRow::from_row(row)
    }
}

impl TableRow {
    pub fn from_row(row: &Row) -> Self {
        // Note: tokenized is nullable. Coerce to emptystring
        let tokenized = row.try_get("tokenized").unwrap_or_default();
        let tableid = get_number(row, "tableid") as u32;
        let colid = get_number(row, "colid") as u32;
        let rowid = get_number(row, "rowid") as u64;

        TableRow {
            tokenized,
            tableid,
            colid,
            rowid,
        }
    }

    pub fn from_bin(data: &[u8]) -> Result<(Self, &[u8])> {
        let (total_length, rest) = decompress(data);
        let total_length = total_length as usize;

        if rest.len() < total_length {
            return Err(anyhow::Error::msg("need more data"));
        }

        let v = TableRow::from_bin_raw(rest);

        Ok((v, &rest[total_length..]))
    }

    pub fn from_bin_raw(data: &[u8]) -> Self {
        let (n, rest) = decompress(data);
        let n = n as usize;
        let tokenized = &rest[..n];
        let tokenized = tokenized.smaz_decompress().unwrap();
        let tokenized = String::from_utf8(tokenized).unwrap();

        let ([tableid, colid, rowid], _rest) = decompress_n(&rest[n..]);

        let tableid = tableid as u32;
        let colid = colid as u32;
        let rowid = rowid as u64;

        Self {
            tokenized,
            tableid,
            colid,
            rowid,
        }
    }
}
