#ifndef KANTAPLUS_KEYDIR_HPP
#define KANTAPLUS_KEYDIR_HPP

#include <cstdint>
#include <string>
#include <unordered_map>
#include <mutex>

struct KeyMeta {
	std::int32_t timestamp;
	std::uint32_t val_size;
	std::int64_t val_offset;
	std::uint32_t file_id;
};

class KeyDir {
public:
	void put(const std::string& key, KeyMeta data);
	KeyMeta* get(const std::string& key);
	void del(const std::string& key);
	KeyDir();
private:
	std::unordered_map<std::string, KeyMeta> m_entries;
	std::mutex m_lock;
};

#endif
