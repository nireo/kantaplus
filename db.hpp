#ifndef KANTAPLUS_DB_HPP
#define KANTAPLUS_DB_HPP

#include "memtable.hpp"
#include "sstable.hpp"
#include <bits/stdint-intn.h>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class DB {
public:
	std::string get(const std::string &key);
	void put(const std::string &key, const std::string &val);
	void del(const std::string &key);
	void graceful_shutdown();
	void set_maximum_size(int64_t new_size);
	void start();
	int32_t get_flush_queue_size() const;

	// using the default directory of './kantaplus'
	DB();

	// using a custom directory
	explicit DB(const std::string &directory);

private:
	std::mutex m_ss_mutex;
	std::vector<SSTable> m_sstables;

	std::mutex m_flush_mutex;
	std::vector<Memtable> m_flush_queue;

	std::mutex m_memtable_mutex;
	Memtable m_memtable;
	std::string m_directory;

	int64_t m_maximum_size;

	// a identifier indicating if the database has been setup successfully.
	bool m_ok;

	bool m_running;

	int32_t get_sstable_index(const std::string &filename) const;
	void copy_file(const std::string &src, const std::string &dst);
	void merge_file(const std::string &file1, const std::string &file2);

	// write the contents of the memtable flush queue into sstables
	void write_flush_queue();
	void flush_current_memtable();

	// find all the persistent log files and the sstables.
	void parse_directory();
};

#endif // KANTAPLUS_DB_HPP
