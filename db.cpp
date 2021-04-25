#include "db.hpp"
#include "sstable.hpp"
#include <bits/stdint-intn.h>
#include <cstdio>
#include <dirent.h>
#include <iostream>
#include <string.h>
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
		for (auto it = m_flush_queue.rbegin(); it != m_flush_queue.rend(); ++it) {
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

// set_maximum_size sets maximum size of memtable in bytes
void DB::set_maximum_size(int64_t new_size) { m_maximum_size = new_size; }

// get_flush_queue_size returns the current size of the flush queue
int32_t DB::get_flush_queue_size() const {
	return (int32_t)m_flush_queue.size();
}

// get_sstable_index loops over the sstables and returns the index of the
// sstable with the given filename.
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
	for (auto it = m_flush_queue.rbegin(); it != m_flush_queue.rend(); ++it) {
		// create new sstable
		auto sstable = SSTable(m_directory);
		sstable.write_map(it->get_keymap());

		m_ss_mutex.lock();
		m_sstables.insert(m_sstables.begin(), sstable);
		m_ss_mutex.unlock();
	}

	m_flush_mutex.unlock();
}

// copy_file copies a file from src path to the dst path
void DB::copy_file(const std::string &src, const std::string &dst) {
	std::ifstream src_file;
	std::ofstream dst_file;

	src_file.open(src, std::ios::in | std::ios::binary);
	dst_file.open(dst, std::ios::out | std::ios::binary);
	dst_file << src_file.rdbuf();

	src_file.close();
	dst_file.close();
}

// merge_file takes in two sstables as merges the values in them to form a
// single sstable
void DB::merge_file(const std::string &file1, const std::string &file2) {
	auto full_path1 = m_directory + "/" + file1;
	auto full_path2 = m_directory + "/" + file2;

	// copy the files such that we can get values from the database whilst they're
	// merging
	m_ss_mutex.lock();
	copy_file(full_path1, full_path1 + ".tmp");
	copy_file(full_path2, full_path2 + ".tmp");
	m_ss_mutex.unlock();

	std::map<std::string, std::string> values;

	auto f1_vals = SSTable::get_all_values(file1);
	auto f2_vals = SSTable::get_all_values(file2);

	// this function assumes that file1 is the more recent one
	for (auto &entry : f2_vals) {
		if (entry.value == "\00")
			continue;

		values[entry.key] = entry.value;
	}

	for (auto &entry : f1_vals) {
		if (entry.value == "\00")
			continue;

		values[entry.key] = entry.value;
	}

	std::ofstream ofs(full_path1 + ".tmpf",
										std::ofstream::out | std::ofstream::binary);
	for (auto &[key, value] : values) {
		const KVPair pair{key, value};

		auto as_bytes = Memtable::to_bytes(pair);
		std::copy(as_bytes.cbegin(), as_bytes.cend(),
							std::ostreambuf_iterator<char>(ofs));
	}

	ofs.close();
	if (!ofs) {
		std::cout << "error while writing to file" << std::endl;
	}
	auto f2_index = get_sstable_index(file2);
	m_ss_mutex.lock();
	remove(full_path1.c_str());
	remove(full_path2.c_str());

	m_sstables.erase(m_sstables.begin() + f2_index);

	copy_file(full_path1 + ".tmpf", full_path1);
	m_ss_mutex.unlock();
}

// has_extension checks if str has a given suffix
bool has_extension(const char *str, const char *suffix) {
	if (!str || !suffix)
		return false;

	size_t lenstr = strlen(str);
	size_t lensuffix = strlen(suffix);

	if (lensuffix > lenstr)
		return false;
	return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

// parse_directory takes care of parsing existing files in the directory.
void DB::parse_directory() {
	DIR *dir;
	struct dirent *diread;

	if ((dir = opendir(m_directory.c_str())) != nullptr) {
		while ((diread = readdir(dir)) != nullptr) {
			// check if file is sstable.
			if (has_extension(diread->d_name, ".ss")) {
				m_sstables.push_back(SSTable(m_directory, diread->d_name));
			} else if (has_extension(diread->d_name, ".log")) {
				m_flush_queue.push_back(Memtable(m_directory, diread->d_name));
			}
		}
		closedir(dir);
	}
}

// compact_n_files compacts n sstables into a single combined sstable. It also
// removes all of the older files.
void DB::compact_n_files(int32_t n) {
	if (n > (int32_t)m_sstables.size())
		return;

	std::string most_recent_file;
	std::map<std::string, std::string> final_values;

	for (auto it = m_sstables.rbegin(); it != m_sstables.rend() - n; ++it) {
		most_recent_file = it->get_filename();
		auto values = it->get_all_values(it->get_filename());

		for (auto &kv_pair : values)
			final_values[kv_pair.key] = kv_pair.value;
	}

	m_ss_mutex.lock();
	int original_size = (int)m_sstables.size();
	for (int i = original_size - 1; i >= original_size - n; --i) {
		// remove the old files
		remove(m_sstables[i].get_filename().c_str());

		// remove from the array
		m_sstables.erase(m_sstables.begin() + i);
	}

	// create the compacted sstable and add it to the vector
	auto sstable = SSTable();
	sstable.write_map(final_values);
	m_sstables.push_back(sstable);

	m_ss_mutex.unlock();
}

int64_t get_file_size(const char *filename) {
	struct stat st;
	if (stat(filename, &st) != 0) {
		return 0;
	}

	return (int64_t)st.st_size;
}

std::vector<std::string> DB::get_compactable_files() const {
	std::vector<std::string> res;
	DIR *dir;
	struct dirent *diread;

	if ((dir = opendir(m_directory.c_str())) != nullptr) {
		while ((diread = readdir(dir)) != nullptr) {
			// check if file is sstable.
			if (has_extension(diread->d_name, ".ss")) {
				if (get_file_size(diread->d_name) < max_sstable_size) {
					res.push_back(diread->d_name);
				}
			}
		}
		closedir(dir);
	}

	return res;
}
