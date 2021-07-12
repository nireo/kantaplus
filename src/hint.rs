use crate::keydir::KeydirEntry;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

pub struct HintFile {
    fp: std::fs::File,
    offset: u64,
    file_id: u64,
    read_only: bool,
}

impl HintFile {
    pub fn new(directory: &str, file_id: u64) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(format!("{}/{}.ht", directory, &file_id))?;

        Ok(Self {
            fp: file,
            offset: 0,
            file_id,
            read_only: false,
        })
    }

    pub fn new_read_only(path: &Path, file_id: u64) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new().read(true).open(path)?;

        Ok(Self {
            fp: file,
            offset: 0,
            file_id,
            read_only: false,
        })
    }

    pub fn write_keydir_entry(
        &mut self,
        entry: &KeydirEntry,
        key: &[u8],
    ) -> Result<(), std::io::Error> {
        let mut wr = BufWriter::new(&self.fp);

        wr.seek(SeekFrom::End(0))?;
        wr.write_all(&entry.encode(key))?;
        wr.flush()?;

        Ok(())
    }

    pub fn next(&mut self) -> Result<(KeydirEntry, Vec<u8>), std::io::Error> {
        // if !self.read_only {
        //     return Err(std::io::Error::new(
        //         std::io::ErrorKind::Other,
        //         "the file is not read-only so will not scan it.",
        //     ));
        // }

        if self.fp.metadata().unwrap().len() + 24 <= self.offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "end of file",
            ));
        }

        let mut reader = BufReader::new(&self.fp);
        reader.seek(SeekFrom::Start(self.offset))?;

        let entry = KeydirEntry::decode(&mut reader, self.file_id)?;
        self.offset += (24 + entry.1.len()) as u64;

        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hint_file() {
        let res = std::fs::create_dir("./tests3");
        assert!(!res.is_err());

        let hintfile = HintFile::new("./tests3", 123123);
        assert!(!hintfile.is_err());

        let mut hintfile = hintfile.unwrap();
        let res = hintfile.write_keydir_entry(
            &KeydirEntry {
                file_id: 123123,
                offset: 0,
                timestamp: 123123123,
                val_size: 10,
            },
            "hello".as_bytes(),
        );
        assert!(!res.is_err());

        let next = hintfile.next();
        if next.is_err() {
            println!("{}", next.as_ref().err().unwrap());
        }
        assert!(!next.is_err());


        let next = next.unwrap();
        println!("{:?}", &next);
        assert_eq!(next.0.offset, 0);
        assert_eq!(next.0.timestamp, 123123123);
        assert_eq!(next.0.val_size, 10);
        assert_eq!(next.0.file_id, 123123);

        let res = std::fs::remove_dir_all("./tests3");
        assert!(!res.is_err());
    }
}
