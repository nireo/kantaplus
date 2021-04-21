//
// Created by eemil on 4/20/21.
//

#include "memtable.hpp"
#include <iostream>

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

    ofs.close();
    if (!ofs) {
        return 1;
    }

    return 0;
}

void Memtable::serialize_uint(unsigned char (&buf)[4], uint32_t val) {
    uint32_t uval = val;
    buf[0] = uval;
    buf[1] = uval >> 8;
    buf[2] = uval >> 16;
    buf[3] = uval >> 24;
}

uint32_t Memtable::parse_uint(unsigned char (&buf)[4]) {
    uint32_t u0 = buf[0], u1 = buf[1], u2 = buf[2], u3 = buf[3];
    uint32_t uval = u0 | (u1 << 8) | (u2 << 16) | (u3 << 24);
    return uval;
}

int Memtable::from_bytes(std::vector<unsigned char> &bytes, KVPair *pair) {
    // the values are separated by one 0-byte byte and it has to 4-byte long
    // integer values, meaning that the length needs to be at least 9.
    if (bytes.size() < 9) {
        return 1;
    }

    // construct buffers to get the key and value sizes
    unsigned char key_length_buffer[4];
    for (int i = 1; i <= 4; ++i)
        key_length_buffer[i-1] = bytes[i];

    unsigned char val_length_buffer[4];
    for (int i = 5; i < 9; ++i)
        val_length_buffer[i-5] = bytes[i];

    auto key_length = Memtable::parse_uint(reinterpret_cast<unsigned char (&)[4]>(key_length_buffer));
    auto val_length = Memtable::parse_uint(reinterpret_cast<unsigned char (&)[4]>(val_length_buffer));

    printf("key length: %d\n", key_length);
    printf("value length: %d\n", val_length);

    std::vector<unsigned char> key = std::vector<unsigned char>(
            bytes.begin() + 9, bytes.begin()+9+key_length);
    std::vector<unsigned char> value = std::vector<unsigned char>(
            bytes.begin() + 9 + key_length, bytes.begin()+9+key_length+val_length);


    auto key_str = std::string(key.begin(), key.end());
    auto val_str = std::string(value.begin(), value.end());
    std::cout << "key: " << key_str << "\n";
    std::cout << "key: " << val_str << "\n";

    pair->key = key_str;
    pair->value = val_str;

    return 0;
}

std::vector<unsigned char> Memtable::to_bytes(const KVPair &pair) {
    // each entry is separated by a 0-byte
    std::vector<unsigned char> res(1, (unsigned char)0);

    // get the key and value lengths as bytes.
    char key_buffer[4];
    char val_buffer[4];

    serialize_uint(reinterpret_cast<unsigned char (&)[4]>(key_buffer),
                   (uint32_t)pair.key.size());
    serialize_uint(reinterpret_cast<unsigned char (&)[4]>(val_buffer),
                   (uint32_t)pair.value.size());

    res.insert(res.end(), key_buffer, key_buffer+4);
    res.insert(res.end(), val_buffer, val_buffer+4);

    for (auto& b : pair.key)
        res.push_back((unsigned char)b);
    for (auto& b : pair.value)
        res.push_back((unsigned char)b);

    return res;
}

