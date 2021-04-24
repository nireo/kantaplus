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
#include <map>
#include <string>

class SSTable {
public:
	// write_map takes in the value map from the memtable and then writen that
	// value to disk.
	void write_map(std::map<std::string, std::string> &mp) const;
	std::string get(const std::string &key) const;
	SSTable();
	SSTable(const std::string &directory);

private:
	// TODO: add a bloom filter and a sparse index
	std::string filename;
};

#endif // KANTAPLUS_SSTABLE_HPP
