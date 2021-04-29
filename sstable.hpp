//
// Created by eemil on 4/21/21.
//

#ifndef KANTAPLUS_SSTABLE_HPP
#define KANTAPLUS_SSTABLE_HPP

#include "memtable.hpp"
#include "rbtree.hpp"
#include <chrono>
#include <fstream>
#include <iostream>
#include <istream>
#include <map>
#include <string>

// a helper class for scanning through values in an sstable.
class SSTableScanner {
public:
	// go the next value in the file
	void next();
	KVPair *get_current_pair() const;

	SSTableScanner(const std::string& filename);
	~SSTableScanner();
private:
	std::string m_filename;
	int64_t m_pos;
	KVPair *m_current;
	std::ifstream *m_in;
};

class SSTable {
public:
	// write_map takes in the value map from the memtable and then writen that
	// value to disk.
	void write_map(std::map<std::string, std::string> &mp) const;
	std::string get_filename() const;

	std::string get(const std::string &key) const;
	SSTable();
	SSTable(const std::string &directory);

	// index a existing sstable
	SSTable(const std::string &directory, const std::string &filename);

	SSTableScanner create_scanner();

	static std::vector<KVPair> get_all_values(const std::string &path);
private:
	// TODO: add a bloom filter and a sparse index
	std::string filename;
};


#endif // KANTAPLUS_SSTABLE_HPP
