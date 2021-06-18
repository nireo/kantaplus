use std::time::{SystemTime, UNIX_EPOCH};

pub struct Entry {
		timestamp: u64,
		key_size: u32,
		value_size: u64,
		key: Vec<u8>,
		value: Vec<u8>,
}

impl Entry {
		pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
				let timestamp = SystemTime::now()
						.duration_since(UNIX_EPOCH)
						.unwrap()
						.as_secs();

				Self {
						timestamp,
						key_size: key.len() as u32,
						value_size: value.len() as u64,
						key,
						value,
				}
		}

		pub fn encode(&self) -> Vec<u8> {
				let mut buffer = vec![];

				buffer.extend_from_slice(&self.timestamp.to_be_bytes());
				buffer.extend_from_slice(&self.key_size.to_be_bytes());
				buffer.extend_from_slice(&self.value_size.to_be_bytes());
				buffer.extend_from_slice(&self.key);
				buffer.extend_from_slice(&self.value);

				buffer
		}
}
