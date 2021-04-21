//
// Created by eemil on 4/21/21.
//

#ifndef KANTAPLUS_SSTABLE_HPP
#define KANTAPLUS_SSTABLE_HPP

#include <string>
#include <chrono>
#include <map>
#include <fstream>
#include <iostream>
#include "rbtree.hpp"
#include "memtable.hpp"

class SSTable {
public:
    // write_map takes in the value map from the memtable and then writen that value
    // to disk.
    void write_map(std::map<std::string, std::string> &mp) const;
    std::string get(const std::string& key) const;
    SSTable();
private:
    // TODO: add a bloom filter and a sparse index
    std::string filename;
};


#endif //KANTAPLUS_SSTABLE_HPP
