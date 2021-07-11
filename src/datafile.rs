use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::encoder::{self, Entry};
use crate::hint::HintFile;
use crate::keydir::KeydirEntry;

pub struct Datafile {
    read_only: bool,
    pub id: u64,
    fp: std::fs::File,

    // we need the hint file such that we can write into it.
    hint_file: Option<HintFile>,
}

#[derive(PartialEq)]
pub enum FileType {
    DataFile,
    HintFile
}

pub fn parse_file_id(path: &str) -> (u64, FileType) {
    // take the last file, such that we don't accidentally take
    let filename = path.split("/").collect::<Vec<&str>>();
    let filename = filename[filename.len() - 1];

    let parts = filename.split(".").collect::<Vec<&str>>();
    assert!(parts.len() == 2);

    let mut file_type = FileType::DataFile;
    if parts[1] == ".ht" {
        file_type = FileType::HintFile;
    }

    (parts[0].parse::<u64>().unwrap(), file_type)
}

impl Datafile {
    pub fn new(directory: &str) -> Result<Self, std::io::Error> {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(format!("{}/{}.df", directory, id))?;

        let hint_file = HintFile::new(directory, id)?;

        Ok(Self {
            read_only: false,
            fp: file,
            id,
            hint_file: Some(hint_file),
        })
    }

    // take file_id as param since this function is used when we already know the file id so no
    // use in parsing it again.
    pub fn new_read_only(path: &Path, file_id: u64) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .unwrap();

        Ok(Self {
            read_only: true,
            id: file_id,
            fp: file,
            // we don't need the hint file in the read-only versions, since we don't write
            // to the files.
            hint_file: None,
        })
    }

    // returns the offset of the given entry.
    pub fn write_entry(&mut self, entry: &encoder::Entry) -> Result<u64, std::io::Error> {
        let mut wr = BufWriter::new(&self.fp);
        let offset = wr.seek(SeekFrom::End(0))?;
        wr.write_all(&entry.encode())?;
        wr.flush()?;

        Ok(offset)
    }

    pub fn write(&mut self, key: &[u8], value: Vec<u8>) -> Result<KeydirEntry, std::io::Error> {
        if self.read_only {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "cannot write into a read-only file."
            ));
        }

        let entry = encoder::Entry::new(key.to_vec(), value);

        let mut wr = BufWriter::new(&self.fp);
        let offset = wr.seek(SeekFrom::End(0))?;
        wr.write_all(&entry.encode())?;
        wr.flush()?;

        // write into the hint file also.
        let keydir_entry = KeydirEntry {
            file_id: self.id,
            timestamp: entry.timestamp,
            val_size: entry.value_size,
            offset,
        };

        self.hint_file.as_mut().unwrap().write_keydir_entry(&keydir_entry, &entry.key)?;
        Ok(keydir_entry)
    }

    pub fn read_whole_entry_from_offset(&self, offset: u64) -> Result<Entry, std::io::Error> {
        let mut reader = BufReader::new(&self.fp);
        reader.seek(std::io::SeekFrom::Start(offset))?;

        let entry = Entry::decode(&mut reader)?;
        Ok(entry)
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_is_created() {
        let res = std::fs::create_dir("./tests");
        assert!(!res.is_err());

        let res = Datafile::new("./tests");
        assert!(!res.is_err());

        let paths = std::fs::read_dir("./tests").unwrap();
        assert_eq!(paths.count(), 2);

        let res = std::fs::remove_dir_all("./tests");
        assert!(!res.is_err());
    }

    #[test]
    fn read_and_write() {
        let res = std::fs::create_dir("./tests1");
        assert!(!res.is_err());

        let df = Datafile::new("./tests1");
        assert!(!df.is_err());
        let mut df = df.unwrap();

        let entry1 = encoder::Entry::new(
            "key".to_string().into_bytes(),
            "value".to_string().into_bytes(),
        );

        let entry2 = encoder::Entry::new(
            "key".to_string().into_bytes(),
            "value".to_string().into_bytes(),
        );

        let res = df.write_entry(&entry1);
        assert!(!res.is_err());

        let read_entry1 = df.read_whole_entry_from_offset(res.unwrap());
        if read_entry1.is_err() {
            println!("{:?}", read_entry1.err());
        }

        let res = df.write_entry(&entry2);
        assert!(!res.is_err());

        let read_entry2 = df.read_whole_entry_from_offset(res.unwrap());
        assert!(!read_entry2.is_err());

        let res = std::fs::remove_dir_all("./tests1");
        assert!(!res.is_err());
    }
}
