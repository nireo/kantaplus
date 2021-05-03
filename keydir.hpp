#ifndef KANTAPLUS_KEYDIR_HPP
#define KANTAPLUS_KEYDIR_HPP

#include <cstdint>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

struct KeyMeta {
	std::int32_t timestamp;
	std::uint32_t val_size;
	std::int64_t val_offset;
	std::uint32_t file_id;

	std::string to_string() {
		std::ostringstream ss;
		ss << "timestamp: " + std::to_string(timestamp) << "\n";
		ss << "val_size: " + std::to_string(timestamp) << "\n";
		ss << "timestamp: " + std::to_string(timestamp) << "\n";
		ss << "timestamp: " + std::to_string(timestamp) << "\n";

		return ss.str();
	};

	bool is_newer_than_deep(KeyMeta &km) {
		if (km.timestamp < timestamp)
			return true;
		else if (km.timestamp > timestamp) {
			return false;
		}

		if (km.file_id < file_id)
			return true;
		else if (km.file_id > file_id)
			return false;

		if (km.val_offset < val_offset)
			return true;
		else if (km.val_offset > val_offset)
			return false;

		return false;
	};

	bool is_newer_than(KeyMeta &km) {
		if (km.timestamp < timestamp)
			return true;
		else if (km.timestamp > timestamp) {
			return false;
		}

		return false;
	}
};

class KeyDir {
public:
	void put(const std::string &key, KeyMeta data);
	KeyMeta *get(const std::string &key);
	void del(const std::string &key);
	void update_file_id(std::uint32_t oldID, std::uint32_t newID);

	KeyDir();
	KeyDir(const std::string &dir);
private:
	std::unordered_map<std::string, KeyMeta> m_entries;
	std::mutex m_lock;
};

#endif
