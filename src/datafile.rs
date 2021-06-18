use std::fs::File;
use std::io::{Seek, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::encoder;

pub struct Datafile {
		read_only: bool,
		id: u64,
		fp: std::fs::File,
		offset: u64,
}

impl Datafile {
		pub fn new(directory: &str) -> Result<Self, std::io::Error> {
				let id = SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.unwrap()
						.as_secs();

				let file = File::create(format!("{}/{}.df", directory, id))?;

				Ok(Self {
						offset: 0,
						read_only: false,
						fp: file,
						id,
				})
		}

		pub fn write(&mut self, entry: &encoder::Entry) -> Result<(), std::io::Error> {
				self.fp.seek(std::io::SeekFrom::Start(self.offset))?;

				let bytes = entry.encode();

				self.offset += bytes.len() as u64;
				self.fp.write(&bytes)?;

				Ok(())
		}
}
