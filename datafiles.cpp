#include "datafiles.hpp"
#include <cstdint>

Datafile::Datafile(const std::string &dir, std::uint32_t timestamp) {
	m_filename = dir + "/" + std::to_string(timestamp);
}

Status Datafile::insert(std::int64_t time_since_start, const std::string &key,
							const std::string &value) {
	return Status::OK;
}

Status get(std::uint64_t offset, std::uint32_t val_size) {
	return Status::OK;
}
