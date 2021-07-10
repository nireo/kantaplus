use std::fs::OpenOptions;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::Path;

use crate::keydir::KeydirEntry;

pub struct HintFile {
    fp: std::fs::File,
    offset: u64,
    file_id: u64,
}

impl HintFile {
    pub fn new(path: &Path, file_id: u64) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new().read(true).open(path).unwrap();

        Ok(Self {
            fp: file,
            offset: 0,
            file_id,
        })
    }

    pub fn next(&mut self) -> Result<(KeydirEntry, Vec<u8>), std::io::Error> {
        if self.fp.metadata().unwrap().len() <= self.offset - 24 {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "end of file"));
        }

        let mut reader = BufReader::new(&self.fp);
        reader.seek(SeekFrom::Start(self.offset))?;

        let entry = KeydirEntry::decode(&mut reader, self.file_id)?;
        self.offset += (24 + entry.1.len()) as u64;

        Ok(entry)
    }
}
