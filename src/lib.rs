mod datafile;
mod encoder;
mod keydir;

use std::{collections::HashMap, fs};

use datafile::Datafile;
use keydir::{Keydir, KeydirEntry};

#[derive(Clone)]
pub struct Config {
		dir: String,
}

pub struct DB {
		pub config: Config,
		pub keydir: Keydir,
		// mapping the ids into datafiles
		pub manager: HashMap<u32, Datafile>,
		pub write_file: Datafile,
}

impl Config {
		pub fn new(directory: &str) -> Self {
				Self {
						dir: directory.to_owned(),
				}
		}
}

impl DB {
		pub fn new(conf: Config) -> Result<Self, &'static str> {
				let res = fs::create_dir(&conf.dir);
				if res.is_err() {
						return Err("could not create directory");
				}

				Ok(Self {
						config: conf.clone(),
						keydir: Keydir::new(),
						manager: HashMap::new(),
						write_file: Datafile::new(&conf.dir).unwrap(),
				})
		}

		pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), &'static str> {
				let key_entry: KeydirEntry = match self.write_file.write(&key, value) {
						Ok(entry) => entry,
						Err(_) => return Err("could not write value into database"),
				};

				self.keydir.add_entry(&key, key_entry);

				Ok(())
		}
}

#[cfg(test)]
mod tests {
		use super::*;

		#[test]
		fn directory_is_created() {
				let configuration: Config = Config::new("test_dir");
				let db = DB::new(configuration);

				assert!(!db.is_err());

				// make sure a directory exists
				let exists: bool = std::path::Path::new("test_dir").is_dir();
				assert!(exists);

				let res = fs::remove_dir("test_dir");
				assert!(!res.is_err())
		}
}
