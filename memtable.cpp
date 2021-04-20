//
// Created by eemil on 4/20/21.
//

#include "memtable.hpp"

const std::string &Memtable::get_log_path() const {
    return log_path;
}

const std::string& Memtable::get(const std::string &key) const {
    return kvs.at(key);
}

void Memtable::put(const std::string &key, const std::string &value) {
    int ok = write_to_log(key, value);
    if (ok) {
        kvs[key] = value;
    } else {
        perror("could not write to log file");
    }
}

int32_t Memtable::write_to_log(const std::string& key, const std::string& value) const {
    std::ofstream ofs(log_path, std::ofstream::out | std::ofstream::binary);
    ofs.write(key.c_str(), key.size());
    ofs.write(value.c_str(), value.size());

    ofs.close();
    if (!ofs) {
        return 1;
    }

    return 0;
}
