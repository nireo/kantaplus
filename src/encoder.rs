use std::io::BufRead;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Entry {
		pub timestamp: u64,
		pub key_size: u32,
		pub value_size: u32,
		pub key: Vec<u8>,
		pub value: Vec<u8>,
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
						value_size: value.len() as u32,
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

		// https://stackoverflow.com/questions/39464237/whats-the-idiomatic-way-to-reference-bufreader-bufwriter-when-passing-it-between
		pub fn decode<R: BufRead>(r: &mut R) -> Result<Self, std::io::Error> {
				let mut timestamp_buf = [0_u8; 8];
				r.read_exact(&mut timestamp_buf)?;
				let timestamp = u64::from_be_bytes(timestamp_buf);

				let mut key_size_buf = [0_u8; 4];
				r.read_exact(&mut key_size_buf)?;
				let key_size = u32::from_be_bytes(key_size_buf);

				let mut val_size_buf = [0_u8; 4];
				r.read_exact(&mut val_size_buf)?;
				let value_size = u32::from_be_bytes(val_size_buf);

				let mut key_buf = vec![0_u8; key_size as usize];
				r.read_exact(&mut key_buf)?;

				let mut val_buf = vec![0_u8; value_size as usize];
				r.read_exact(&mut val_buf)?;

				Ok(Self {
						timestamp,
						key_size,
						value_size,
						key: key_buf,
						value: val_buf,
				})
		}
}
