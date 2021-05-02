#include "kantadb.hpp"
#include "keydir.hpp"
#include <dirent.h>
#include <iostream>
#include <sys/stat.h>

DB::DB(const std::string &path) {
	m_directory = path;
	DIR *dir = opendir(path.c_str());
	if (dir) {
		closedir(dir);
	} else if (ENOENT == errno) {
		int ok;
		ok = mkdir(m_directory.c_str(), 0777);
		if (!ok) {
			m_ok = false;
		} else {
			m_ok = true;
		}
	}
	m_debug_mode = false;
}

void DB::debug_print(const std::string &to_print) {
	if (m_debug_mode) {
		std::cout << to_print << "\n";
	}
}

DB::DB(const std::string &path, bool debug_mode) {
	m_directory = path;
	DIR *dir = opendir(path.c_str());
	if (dir) {
		debug_print("found database directory parsing files...");
		closedir(dir);
	} else if (ENOENT == errno) {
		int ok;
		ok = mkdir(m_directory.c_str(), 0777);
		if (!ok) {
			debug_print("could not create directory for database");
			m_ok = false;
		} else {
			m_ok = true;
		}
	}

	m_debug_mode = debug_mode;
}
