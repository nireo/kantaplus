use std::collections::BTreeMap;
use std::io::BufRead;

#[derive(Clone, Copy, Debug)]
pub struct KeydirEntry {
    pub file_id: u64,
    pub val_size: u32,
    pub offset: u64,
    pub timestamp: u64,
}

impl KeydirEntry {
    pub fn encode(&self, key: &[u8]) -> Vec<u8> {
        let mut buffer = vec![];

        // we don't need to encode the file id, since the datafile id is the same as the file in which the hints
        // are stored in.
        buffer.extend_from_slice(&self.timestamp.to_be_bytes());
        buffer.extend_from_slice(&key.len().to_be_bytes());
        buffer.extend_from_slice(&self.val_size.to_be_bytes());
        buffer.extend_from_slice(&self.offset.to_be_bytes());
        buffer.extend_from_slice(key);

        buffer
    }

    pub fn decode<R: BufRead>(r: &mut R, file_id: u64) -> Result<(Self, Vec<u8>), std::io::Error> {
        let mut timestamp_buf = [0_u8; 8];
        r.read_exact(&mut timestamp_buf)?;
        let timestamp = u64::from_be_bytes(timestamp_buf);

        let mut key_size_buf = [0_u8; 4];
        r.read_exact(&mut key_size_buf)?;
        let key_size = u32::from_be_bytes(key_size_buf);

        let mut val_size_buf = [0_u8; 4];
        r.read_exact(&mut val_size_buf)?;
        let val_size = u32::from_be_bytes(val_size_buf);

        let mut offset_buf = [0_u8; 8];
        r.read_exact(&mut offset_buf)?;
        let offset = u64::from_be_bytes(offset_buf);

        let mut key_buf = vec![0_u8; key_size as usize];
        r.read_exact(&mut key_buf)?;

        Ok((
            Self {
                timestamp,
                file_id,
                offset,
                val_size,
            },
            key_buf,
        ))
    }
}

pub struct Keydir {
    entries: BTreeMap<Vec<u8>, KeydirEntry>,
}

impl Keydir {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::default(),
        }
    }

    pub fn add(
        &mut self,
        key: &[u8],
        vsize: u32,
        file_id: u64,
        offset: u64,
        ts: u64,
    ) -> Result<(), &'static str> {
        self.entries.insert(
            key.to_vec(),
            KeydirEntry {
                file_id,
                offset,
                timestamp: ts,
                val_size: vsize,
            },
        );

        Ok(())
    }

    pub fn add_entry(&mut self, key: &[u8], entry: KeydirEntry) {
        self.entries.insert(key.to_vec(), entry);
    }

    pub fn get(&self, key: &[u8]) -> Result<KeydirEntry, String> {
        // check if key is in
        match self.entries.get(key) {
            None => Err(String::from("Could not find key in keydir")),
            Some(val) => Ok(val.clone()),
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<(), String> {
        match self.entries.remove(key) {
            Some(_) => Ok(()),
            None => Err(String::from("key was not found in the key directory")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_insert_and_get() {
        let mut keydir = Keydir::new();
        let res = keydir.add("helloworld".as_bytes(), 1, 1, 1, 1);
        assert!(!res.is_err());

        // get the value from the keydir
        let value = keydir.get("helloworld".as_bytes());
        assert!(!value.is_err());
        let value: KeydirEntry = value.unwrap();

        assert_eq!(value.file_id, 1);
        assert_eq!(value.offset, 1);
        assert_eq!(value.timestamp, 1);
        assert_eq!(value.val_size, 1);
    }

    #[test]
    fn cannot_delete_non_existing_key() {
        let mut keydir = Keydir::new();

        assert!(keydir.remove("hello".as_bytes()).is_err());
    }

    #[test]
    fn delete_value() {
        let mut keydir = Keydir::new();

        let res = keydir.add("helloworld1".as_bytes(), 1, 1, 1, 1);
        assert!(!res.is_err());

        let res = keydir.add("helloworld2".as_bytes(), 1, 1, 1, 1);
        assert!(!res.is_err());

        assert!(!keydir.remove("helloworld1".as_bytes()).is_err());
        assert!(!keydir.remove("helloworld2".as_bytes()).is_err());

        let value = keydir.get("helloworld1".as_bytes());
        assert!(value.is_err());

        let value = keydir.get("helloworld2".as_bytes());
        assert!(value.is_err());
    }
}
