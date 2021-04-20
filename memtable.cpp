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

void Memtable::serialize_uint(char (&buf)[4], uint32_t val) {
    uint32_t uval = val;
    buf[0] = uval;
    buf[1] = uval >> 8;
    buf[2] = uval >> 16;
    buf[3] = uval >> 24;
}

uint32_t Memtable::parse_uint(const char (&buf)[4]) {
    uint32_t u0 = buf[0], u1 = buf[1], u2 = buf[2], u3 = buf[3];
    uint32_t uval = u0 | (u1 << 8) | (u2 << 16) | (u3 << 24);
    return uval;
}

std::vector<std::byte> Memtable::to_bytes(const std::string &key, const std::string &value) {
    // each byte is separated by a 0-byte
    std::vector<std::byte> res(1, (std::byte)0);

    // get the key and value lengths as bytes.
    char key_buffer[4];
    char val_buffer[4];

    serialize_uint(reinterpret_cast<char (&)[4]>(key_buffer), (uint32_t)key.size());
    serialize_uint(reinterpret_cast<char (&)[4]>(val_buffer), (uint32_t)value.size());

    for (auto& b : key)
        res.push_back((std::byte)b);
    for (auto& b : value)
        res.push_back((std::byte)b);

    return res;
}