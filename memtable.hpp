//
// Created by eemil on 4/20/21.
//

#ifndef KANTAPLUS_MEMTABLE_HPP
#define KANTAPLUS_MEMTABLE_HPP

#include <map>
#include <string>
#include <fstream>
#include <vector>

// Memtable represents values held in memory before written
// into sstables.
class Memtable {
public:
    [[nodiscard]] const std::string& get_log_path() const;
    [[nodiscard]] int32_t write_to_log(const std::string& key, const std::string& value) const;
    [[nodiscard]] const std::string& get(const std::string& key) const;

    static void serialize_uint(char (&buf)[4], uint32_t val) ;
    static uint32_t parse_uint(const char (&buf)[4]);
    static std::vector<std::byte> to_bytes(const std::string& key, const std::string& value);

    void put(const std::string& key, const std::string& value);
private:
    // TODO: implement a rb tree
    std::map<std::string, std::string> kvs;
    std::string log_path;
    int64_t size;
};


#endif //KANTAPLUS_MEMTABLE_HPP
