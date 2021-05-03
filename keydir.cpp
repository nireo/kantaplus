#include "keydir.hpp"
#include <cstdint>

KeyDir::KeyDir() {
	m_entries = std::unordered_map<std::string, KeyMeta>();
}

void KeyDir::put(const std::string &key, KeyMeta data) {
	m_lock.lock();
	m_entries[key] = data;
	m_lock.unlock();
}

KeyMeta* KeyDir::get(const std::string& key) {
	auto val = m_entries.find(key);
	if (val != m_entries.end())
		return &val->second;
	return nullptr;
}

void KeyDir::del(const std::string &key) {
	m_lock.lock();
	m_entries.erase(key);
	m_lock.unlock();
}

void KeyDir::update_file_id(std::uint32_t oldID, std::uint32_t newID) {
	m_lock.lock();

	for (auto kv : m_entries) {
		if (kv.second.file_id == oldID) {
			kv.second.file_id = newID;
		}
	}

	m_lock.unlock();
}
