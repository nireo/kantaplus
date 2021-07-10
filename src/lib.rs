mod datafile;
mod encoder;
mod keydir;
mod hint;

use std::{collections::HashMap, fs};

use datafile::Datafile;
use encoder::Entry;
use keydir::{Keydir, KeydirEntry};

#[derive(Clone)]
pub struct Config {
    dir: String,
}

pub struct DB {
    pub config: Config,
    pub keydir: Keydir,
    // mapping the ids into datafiles
    pub manager: HashMap<u64, Datafile>,
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
    pub fn new(conf: Config) -> Result<Self, String> {
        let res = fs::create_dir(&conf.dir);
        if res.is_err() {
            return Err(String::from("could not create directory"));
        }

        Ok(Self {
            config: conf.clone(),
            keydir: Keydir::new(),
            manager: HashMap::new(),
            write_file: Datafile::new(&conf.dir).unwrap(),
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        let key_entry: KeydirEntry = match self.write_file.write(&key, value) {
            Ok(entry) => entry,
            Err(_) => return Err(String::from("could not write value into database")),
        };

        self.keydir.add_entry(&key, key_entry);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Entry, String> {
        // first see if the entry is found in the keydir.
        let kd_entry = self.keydir.get(key)?;

        // check if the entry is written in the writable file into the stored files.
        if kd_entry.file_id == self.write_file.id {
            let entry = self
                .write_file
                .read_whole_entry_from_offset(kd_entry.offset);
            if entry.is_err() {
                return Err(String::from("could not find entry from file"));
            } else {
                return Ok(entry.unwrap());
            }
        }

        let file = self.manager.get(&kd_entry.file_id);
        match file {
            Some(f) => {
                let entry = f.read_whole_entry_from_offset(kd_entry.offset);
                if entry.is_err() {
                    Err(String::from("could not find entry from file"))
                } else {
                    Ok(entry.unwrap())
                }
            }
            None => Err(String::from("couldn't find file in file manager")),
        }
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
        let path = std::path::Path::new("test_dir");
        let exists: bool = path.is_dir();
        assert!(exists);

        let res = fs::remove_dir_all(path);
        assert!(!res.is_err())
    }

    #[test]
    fn test_write_read() {
        let configuration: Config = Config::new("test_dir2");
        let path = std::path::Path::new("test_dir2");
        let db = DB::new(configuration);
        assert!(!db.is_err());
        let mut db = db.unwrap();

        let err = db.put("hello".as_bytes().to_vec(), "world".as_bytes().to_vec());
        assert!(!err.is_err());

        let entry = db.get("hello".as_bytes());
        if entry.is_err() {
            println!("{}", entry.as_ref().err().unwrap());
        }
        assert!(!entry.is_err());
        let entry = entry.unwrap();

        assert_eq!(String::from_utf8_lossy(&entry.value), "world");

        let res = fs::remove_dir_all(path);
        assert!(!res.is_err())
    }
}
