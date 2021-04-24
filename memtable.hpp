//
// Created by eemil on 4/20/21.
//

#ifndef KANTAPLUS_MEMTABLE_HPP
#define KANTAPLUS_MEMTABLE_HPP

#include "rbtree.hpp"
#include <chrono>
#include <fstream>
#include <map>
#include <string>
#include <vector>

// Memtable represents values held in memory before written
// into sstables.
class Memtable {
public:
	[[nodiscard]] const std::string &get_log_path() const;
	[[nodiscard]] int32_t write_to_log(const std::string &key,
																		 const std::string &value) const;
	[[nodiscard]] std::string get(const std::string &key) const;

	static void serialize_uint(unsigned char (&buf)[4], uint32_t val);
	static uint32_t parse_uint(unsigned char (&buf)[4]);
	static std::vector<unsigned char> to_bytes(const KVPair &value);

	void put(const std::string &key, const std::string &value);
	static int from_bytes(std::vector<unsigned char> &bytes, KVPair *entry);
	int64_t get_size() const;
	std::map<std::string, std::string>& get_keymap();

	void read_entries_from_log();

	Memtable();

	// create a memtable from a log file that is inside a given directory
	Memtable(const std::string &path, const std::string& directory);

	// create a empty memtable in the given directory, i.e. database directory
	Memtable(const std::string& directory);

private:
	std::map<std::string, std::string> kvs;
	std::string log_path;
	int64_t size;

	// we need the database directory since the log files will go there
	std::string database_directory;
};

#endif // KANTAPLUS_MEMTABLE_HPP
