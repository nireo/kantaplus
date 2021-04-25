//
// Created by eemil on 4/21/21.
//

#include "sstable.hpp"
#include <bits/stdint-intn.h>

SSTable::SSTable() {
	auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
											 std::chrono::system_clock::now().time_since_epoch())
											 .count();
	filename = std::to_string(timestamp) + ".ss";
}

void SSTable::write_map(std::map<std::string, std::string> &mp) const {
	// open a filestream for writing
	std::ofstream ofs(filename, std::ofstream::out | std::ofstream::binary);

	for (auto &[key, value] : mp) {
		const KVPair pair{key, value};

		auto as_bytes = Memtable::to_bytes(pair);
		std::copy(as_bytes.cbegin(), as_bytes.cend(),
							std::ostreambuf_iterator<char>(ofs));
	}

	ofs.close();
	if (!ofs) {
		std::cout << "error while writing to file" << std::endl;
	}
}

std::vector<KVPair> SSTable::get_all_values(const std::string &path) {
	std::vector<KVPair> res;
	std::ifstream in;
	in.open(path);
	std::vector<unsigned char> bytes((std::istreambuf_iterator<char>(in)),
																	 std::istreambuf_iterator<char>());

	int64_t pos = 0;

	for (;;) {
		if (bytes.size() - pos < 10)
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

		std::vector<unsigned char> key_vector = std::vector<unsigned char>(
				bytes.begin() + 9 + pos, bytes.begin() + 9 + key_length + pos);
		std::vector<unsigned char> value_vector = std::vector<unsigned char>(
				bytes.begin() + 9 + key_length + pos,
				bytes.begin() + 9 + key_length + val_length + pos);

		res.push_back(KVPair{
				.key = std::string(key_vector.begin(), key_vector.end()),
				.value = std::string(value_vector.begin(), value_vector.end()),
		});

		pos += 9 + key_length + val_length;
	}

	return res;
}

std::string SSTable::get(const std::string &key) const {
	std::ifstream in;
	in.open(filename);
	std::vector<unsigned char> bytes((std::istreambuf_iterator<char>(in)),
																	 std::istreambuf_iterator<char>());

	int64_t pos = 0;

	// if a value has not been found it is equal to an emtpy string.
	std::string value;

	for (;;) {
		if (bytes.size() - pos < 10)
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

		std::vector<unsigned char> key_vector = std::vector<unsigned char>(
				bytes.begin() + 9 + pos, bytes.begin() + 9 + key_length + pos);
		auto key_str = std::string(key_vector.begin(), key_vector.end());
		if (key_str == key) {
			// only parse the actual only if we found the key, just a small
			// optimization
			std::vector<unsigned char> val = std::vector<unsigned char>(
					bytes.begin() + 9 + key_length + pos,
					bytes.begin() + 9 + key_length + val_length + pos);
			value = std::string(val.begin(), val.end());
		}
		pos += 9 + key_length + val_length;
	}

	return value;
}

SSTable::SSTable(const std::string &directory) {
	auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
											 std::chrono::system_clock::now().time_since_epoch())
											 .count();
	filename = directory + "/" + std::to_string(timestamp) + ".ss";
}

SSTable::SSTable(const std::string &directory, const std::string &fname) {
	filename = directory + "/" + fname;
}

std::string SSTable::get_filename() const { return filename; }

SSTableScanner::SSTableScanner(const std::string &filename) {
	// the current value is a nullptr also when there isn't anymore values
	m_current = nullptr;
	m_pos = (int64_t)0;
	m_filename = filename;
}

void SSTableScanner::next() {}

KVPair *SSTableScanner::get_current_pair() const { return m_current; }
