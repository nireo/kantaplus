//
// Created by eemil on 4/20/21.
//

#ifndef KANTAPLUS_MEMTABLE_HPP
#define KANTAPLUS_MEMTABLE_HPP

#include <map>
#include <string>
#include <fstream>
#include <vector>
#include "rbtree.hpp"

// Memtable represents values held in memory before written
// into sstables.
class Memtable {
public:
    [[nodiscard]] const std::string& get_log_path() const;
    [[nodiscard]] int32_t write_to_log(const std::string& key, const std::string& value) const;
    [[nodiscard]] const std::string& get(const std::string& key) const;

    static void serialize_uint(unsigned char (&buf)[4], uint32_t val) ;
    static uint32_t parse_uint(unsigned char (&buf)[4]);
    static std::vector<unsigned char> to_bytes(const KVPair& value);

    void put(const std::string& key, const std::string& value);
    static int from_bytes(std::vector<unsigned char> &bytes, KVPair *entry);
private:
    // TODO: implement a rb tree
    std::map<std::string, std::string> kvs;
    std::string log_path;
    int64_t size;

};


#endif //KANTAPLUS_MEMTABLE_HPP
