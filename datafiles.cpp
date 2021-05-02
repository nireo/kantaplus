#include "datafiles.hpp"

Datafile::Datafile(const std::string &path) {
	m_filename = path;
}

Status Datafile::insert(std::int64_t time_since_start, const std::string &key,
							const std::string &value) {
	return Status::OK;
}

Status get(std::uint64_t offset, std::uint32_t val_size) {
	return Status::OK;
}
