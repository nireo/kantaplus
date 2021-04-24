#include "db.hpp"
#include "sstable.hpp"
#include <bits/stdint-intn.h>
#include <iostream>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

// DB(str) creates a database instance given a certain directory for log files
DB::DB(const std::string &directory) {
	int ok;
	ok = mkdir(directory.c_str(), 0777);
	if (!ok) {
		std::cout << "could not start database"
							<< "\n";
		m_ok = false;
	} else {
		m_ok = true;
	}

	m_directory = directory;
	m_memtable = Memtable(directory);
	m_sstables = std::vector<SSTable>();
	m_flush_queue = std::vector<Memtable>();
}

// DB() creates a database in a default directory of './kantadb'
DB::DB() {
	int ok;
	ok = mkdir("./kantadb", 0777);
	if (!ok) {
		m_ok = false;
	} else {
		m_ok = true;
	}

	m_directory = "kantadb";
	m_memtable = Memtable("./kantadb");
	m_sstables = std::vector<SSTable>();
	m_flush_queue = std::vector<Memtable>();
}

// Get finds a value from the database. It firstly checks the current memtable
// after which it checks the flush queue and lastly the sstables.
std::string DB::get(const std::string &key) {
	// search the current memtable
	m_memtable_mutex.lock();
	std::string val = m_memtable.get(key);
	m_memtable_mutex.unlock();

	// if the value was not found in the current memtable search for it
	// in the queue.
	if (val == "") {
		for (auto &table : m_flush_queue) {
			auto tmp = table.get(key);
			if (tmp != "") {
				val = tmp;
			}
		}
	}

	if (val == "") {
		for (auto &sstable : m_sstables) {
			auto tmp = sstable.get(key);
			if (tmp != "")
				val = tmp;
		}
	}

	// check that the vlue isn't deleted
	if (val == "\00")
		return "";

	return val;
}

// write_flush_queue takes in the flush_queue and writes the memtables from the
// queue into sstables on disk.
void DB::write_flush_queue() {
	while (m_running) {
		m_flush_mutex.lock();

		// create sstables for the oldest entries in the queue
		for (auto it = m_flush_queue.rbegin();
				 it != m_flush_queue.rend(); ++it) {
			// create new sstable
			auto sstable = SSTable(m_directory);
			sstable.write_map(it->get_keymap());

			m_ss_mutex.lock();
			m_sstables.insert(m_sstables.begin(), sstable);
			m_ss_mutex.unlock();
		}

		m_flush_mutex.unlock();

		// sleep for 100 microseconds
		usleep(100);
	}
}

// flush_current_memtable resets the current memtable instance and also
// appends the latest memtable to the beginning of the flush_queue.
void DB::flush_current_memtable() {
	m_flush_mutex.lock();
	m_flush_queue.insert(m_flush_queue.begin(), m_memtable);
	m_flush_mutex.unlock();

	m_memtable_mutex.lock();
	m_memtable = Memtable(m_directory);
	m_memtable_mutex.unlock();
}

// Put creates a entry in the database. It firstly checks that the memtable
// is not too big. After that it writes that vaue into the memtable.
void DB::put(const std::string &key, const std::string &value) {
	// 10 mb
	if (m_memtable.get_size() >= m_maximum_size) {
		// after this the new values will be placed into the new memory table
		// since flush_current_memtable updates it.
		flush_current_memtable();
	}

	m_memtable_mutex.lock();
	m_memtable.put(key, value);
	m_memtable_mutex.unlock();
}

// Delete a value. It really doesn't delete the value it only makes an
// indication that a given entry has been deleted. It will get properly
// deleted when sstable compaction happens.
void DB::del(const std::string &key) {
	if (m_memtable.get_size() >= m_maximum_size) {
		// after this the new values will be placed into the new memory table
		// since flush_current_memtable updates it.
		flush_current_memtable();
	}

	m_memtable_mutex.lock();
	m_memtable.put(key, "\00");
	m_memtable_mutex.unlock();
}

// start() starts running the database service and it also handles writing flush
// queue into sstables in a 'flush_thread'.
void DB::start() {
	m_running = true;

	std::thread flush_thread([this] { write_flush_queue(); });
}

void DB::set_maximum_size(int64_t new_size) { m_maximum_size = new_size; }

int32_t DB::get_flush_queue_size() const {
	return (int32_t)m_flush_queue.size();
}

int32_t DB::get_sstable_index(const std::string &filename) const {
	for (int i = 0; i < (int)m_sstables.size(); ++i)
		if (m_sstables[i].get_filename() == filename)
			return i;

	// not found
	return -1;
}

// graceful_shutdown writes all the values into sstables, this isn't needed, but
// just an optional function.
void DB::graceful_shutdown() {
	// place the current memtable into the queue
	flush_current_memtable();

	// flush the queue into sstables.
	m_flush_mutex.lock();

	// create sstables for the oldest entries in the queue
	for (auto it = m_flush_queue.rbegin(); it != m_flush_queue.rend();
			 ++it) {
		// create new sstable
		auto sstable = SSTable(m_directory);
		sstable.write_map(it->get_keymap());

		m_ss_mutex.lock();
		m_sstables.insert(m_sstables.begin(), sstable);
		m_ss_mutex.unlock();
	}

	m_flush_mutex.unlock();
}

void DB::copy_file(const std::string &src, const std::string &dst) {
	std::ifstream src_file;
	std::ofstream dst_file;

	src_file.open(src, std::ios::in | std::ios::binary);
	dst_file.open(dst, std::ios::out | std::ios::binary);
	dst_file << src_file.rdbuf();

	src_file.close();
	dst_file.close();
}

void DB::merge_file(const std::string &file1, const std::string &file2) {
	auto full_path1 = m_directory + "/" + file1;
	auto full_path2 = m_directory + "/" + file2;

	// copy the files such that we can get values from the database whilst they're merging
	m_ss_mutex.lock();
	copy_file(full_path1, full_path1+".tmp");
	copy_file(full_path2, full_path2+".tmp");
	m_ss_mutex.unlock();

	auto to_compact = std::vector<std::string>{full_path1+".tmp", full_path2+".tmp"};
	std::map<std::string, std::string> values;
	for (auto& filename : to_compact) {

	}
}
