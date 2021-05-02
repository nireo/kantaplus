#ifndef KANTAPLUS_DATAFILES_HPP
#define KANTAPLUS_DATAFILES_HPP

#include "rbtree.hpp"
#include <cstdint>
#include <string>

class Datafile {
public:
	Datafile(const std::string& path);
	Status insert(std::int64_t time_since_start, const std::string& key, const std::string& value);
	Status get(std::uint64_t offset, std::uint32_t val_size);
private:
	std::string m_filename;
};

#endif // KANTAPLUS_DB_HPP
