pub struct Datafile {
		read_only: bool,
		id: u64,
		fp: std::fs::File,
}
