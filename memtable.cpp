//
// Created by eemil on 4/20/21.
//

#include "memtable.hpp"
#include <bits/stdint-intn.h>
#include <stdexcept>

const std::string &Memtable::get_log_path() const { return log_path; }

std::string Memtable::get(const std::string &key) const {
	auto val = this->kvs.find(key);
	if (val != this->kvs.end())
		return val->second;
	return "";
}

void Memtable::put(const std::string &key, const std::string &value) {
	int ok = write_to_log(key, value);
	this->kvs[key] = value;

	// so the size of the memtable consists of the first 9 bytes, containing the
	// value separator and the key and value lengths.
	this->size += 9 + static_cast<int64_t>(key.length()) +
								static_cast<int64_t>(value.length());
}

int32_t Memtable::write_to_log(const std::string &key,
															 const std::string &value) const {
	std::ofstream ofs(log_path, std::ofstream::out | std::ofstream::binary);

	auto data = to_bytes(KVPair{
			.key = key,
			.value = value,
	});
	ofs << std::string(data.begin(), data.end());

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
		key_length_buffer[i - 1] = bytes[i];

	unsigned char val_length_buffer[4];
	for (int i = 5; i < 9; ++i)
		val_length_buffer[i - 5] = bytes[i];

	auto key_length = Memtable::parse_uint(
			reinterpret_cast<unsigned char(&)[4]>(key_length_buffer));
	auto val_length = Memtable::parse_uint(
			reinterpret_cast<unsigned char(&)[4]>(val_length_buffer));

	std::vector<unsigned char> key = std::vector<unsigned char>(
			bytes.begin() + 9, bytes.begin() + 9 + key_length);
	std::vector<unsigned char> value =
			std::vector<unsigned char>(bytes.begin() + 9 + key_length,
																 bytes.begin() + 9 + key_length + val_length);

	auto key_str = std::string(key.begin(), key.end());
	auto val_str = std::string(value.begin(), value.end());

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

	serialize_uint(reinterpret_cast<unsigned char(&)[4]>(key_buffer),
								 (uint32_t)pair.key.size());
	serialize_uint(reinterpret_cast<unsigned char(&)[4]>(val_buffer),
								 (uint32_t)pair.value.size());

	res.insert(res.end(), key_buffer, key_buffer + 4);
	res.insert(res.end(), val_buffer, val_buffer + 4);

	for (auto &b : pair.key)
		res.push_back((unsigned char)b);
	for (auto &b : pair.value)
		res.push_back((unsigned char)b);

	return res;
}

void Memtable::read_entries_from_log() {
	std::ifstream in;
	in.open(log_path);
	std::vector<unsigned char> bytes((std::istreambuf_iterator<char>(in)),
																	 std::istreambuf_iterator<char>());

	int64_t pos = 0;
	for (;;) {
		if (bytes.size() - pos < 0)
			break;

		unsigned char key_length_buffer[4];
		for (int i = pos + 1; i <= pos + 4; ++i)
			key_length_buffer[i - 1] = bytes[i];

		unsigned char val_length_buffer[4];
		for (int i = pos + 5; i < pos + 9; ++i)
			val_length_buffer[i - 5] = bytes[i];

		auto key_length = Memtable::parse_uint(
				reinterpret_cast<unsigned char(&)[4]>(key_length_buffer));
		auto val_length = Memtable::parse_uint(
				reinterpret_cast<unsigned char(&)[4]>(val_length_buffer));

		std::vector<unsigned char> key = std::vector<unsigned char>(
				bytes.begin() + 9 + pos, bytes.begin() + 9 + key_length + pos);

		std::vector<unsigned char> value = std::vector<unsigned char>(
				bytes.begin() + 9 + key_length + pos,
				bytes.begin() + 9 + key_length + val_length + pos);

		auto key_str = std::string(key.begin(), key.end());
		auto val_str = std::string(value.begin(), value.end());

		this->kvs[key_str] = val_str;
		pos += 9 + key_length + val_length;
		size += (int64_t)(9 + key_length + val_length);
	}
}

Memtable::Memtable() {
	// get the name for the file as a timestamp
	auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
											 std::chrono::system_clock::now().time_since_epoch())
											 .count();
	log_path = std::to_string(timestamp) + ".log";

	std::ofstream ofs(log_path);
	ofs.close();

	// we use the ofstream to write to the file, which creates the file
	// if it doesn't exist, so we don't need to worry about that.
	kvs = std::map<std::string, std::string>();
	size = 0;
}

Memtable::Memtable(const std::string &path, const std::string &directory) {
	log_path = path;
	read_entries_from_log();
}

Memtable::Memtable(const std::string &directory) {
	// get the name for the file as a timestamp
	auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
											 std::chrono::system_clock::now().time_since_epoch())
											 .count();
	log_path = directory + "/" + std::to_string(timestamp) + ".log";

	std::ofstream ofs(log_path);
	ofs.close();

	// we use the ofstream to write to the file, which creates the file
	// if it doesn't exist, so we don't need to worry about that.
	kvs = std::map<std::string, std::string>();
	size = 0;
}

int64_t Memtable::get_size() const { return this->size; }

std::map<std::string, std::string> &Memtable::get_keymap() { return this->kvs; }
