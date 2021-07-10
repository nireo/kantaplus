use std::fs::OpenOptions;

use crate::keydir::KeydirEntry;

pub struct HintFile {
    fp: std::fs::File,
    offset: u64,
}

impl HintFile {
    pub fn new(path: &str) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new().read(true).open(path).unwrap();

        Ok(Self {
            fp: file,
            offset: 0,
        })
    }
}
