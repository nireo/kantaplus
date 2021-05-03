#ifndef KANTAPLUS_DATAFILES_HPP
#define KANTAPLUS_DATAFILES_HPP

#include "rbtree.hpp"
#include <bits/types/FILE.h>
#include <cstdint>
#include <string>

class Datafile {
public:
	Datafile(const std::string &dir, std::uint32_t timestamp);
	Status insert(std::int64_t time_since_start, const std::string &key,
								const std::string &value);
	Status get(std::uint64_t offset, std::uint32_t val_size);

private:
	std::string m_filename;
	FILE *fp;
};

#endif // KANTAPLUS_DATAFILES_HPP