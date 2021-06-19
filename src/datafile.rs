use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::encoder::{self, Entry};
use crate::keydir::KeydirEntry;

pub struct Datafile {
		read_only: bool,
		id: u64,
		fp: std::fs::File,
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
						.open(format!("{}/{}.df", directory, id))
						.unwrap();

				Ok(Self {
						read_only: false,
						fp: file,
						id,
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
				let entry = encoder::Entry::new(key.to_vec(), value);

				let mut wr = BufWriter::new(&self.fp);
				let offset = wr.seek(SeekFrom::End(0))?;
				wr.write_all(&entry.encode())?;
				wr.flush()?;

				Ok(KeydirEntry {
						file_id: self.id,
						timestamp: entry.timestamp,
						offset: offset + 8 + 4,
						val_size: entry.value_size,
				})
		}

		pub fn read_whole_entry_from_offset(&mut self, offset: u64) -> Result<Entry, std::io::Error> {
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
				assert_eq!(paths.count(), 1);

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
