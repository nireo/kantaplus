#ifndef KANTAPLUS_DB_HPP
#define KANTAPLUS_DB_HPP

#include "datafiles.hpp"
#include "keydir.hpp"
#include "rbtree.hpp"
#include <string>
#include <unordered_map>
#include <vector>

class DB {
public:
	DB(const std::string &path);
	DB(const std::string &path, bool debug_mode);

	Status put(const std::string &key, const std::string &value);
	Status get(const std::string &key);
	Status del(const std::string &key);

private:
	std::string m_directory;
	KeyDir m_keydir;
	std::vector<Datafile> m_datafiles;
	bool m_ok;
	bool m_debug_mode;

	void debug_print(const std::string &to_print);
};

#endif // KANTAPLUS_DB_HPP
