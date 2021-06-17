mod datafile;
mod encoder;
mod keydir;

use std::fs;

#[derive(Clone)]
pub struct Config {
		dir: String,
}

pub struct DB {
		pub config: Config,
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
				})
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
