use std::collections::BTreeMap;

#[derive(Clone, Copy)]
pub struct KeydirEntry {
		pub file_id: u32,
		pub val_size: u64,
		pub offset: u64,
		pub timestamp: u64,
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
				vsize: u64,
				file_id: u32,
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

		pub fn get(&self, key: &[u8]) -> Result<KeydirEntry, &'static str> {
				// check if key is in
				match self.entries.get(key) {
						None => Err("Could not find key in keydir"),
						Some(val) => Ok(val.clone()),
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
}
