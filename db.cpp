#include "db.hpp"
#include "sstable.hpp"
#include <iostream>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

// DB(str) creates a database instance given a certain directory for log files
DB::DB(const std::string& directory) {
  int ok;
  ok = mkdir(directory.c_str(), 0777);
  if (!ok) {
    std::cout << "could not start database" << "\n";
    m_ok = false;
  } else {
    m_ok = true;
  }

  this->m_directory = directory;
  this->m_memtable = Memtable();
  this->m_sstables = std::vector<SSTable>();
  this->m_flush_queue = std::vector<Memtable>();
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

  this->m_directory = "kantadb";
  this->m_memtable = Memtable();
  this->m_sstables = std::vector<SSTable>();
  this->m_flush_queue = std::vector<Memtable>();
}

// Get finds a value from the database. It firstly checks the current memtable
// after which it checks the flush queue and lastly the sstables.
std::string DB::get(const std::string& key) {
  // search the current memtable
  this->m_memtable_mutex.lock();
  std::string val = this->m_memtable.get(key);
  this->m_memtable_mutex.unlock();

  // if the value was not found in the current memtable search for it
  // in the queue.
  if (val == "") {
    for (auto& table : this->m_flush_queue) {
      auto tmp = table.get(key);
      if (tmp != "") {
	val = tmp;
      }
    }
  }

  if (val == "") {
    for (auto& sstable : this->m_sstables) {
      auto tmp = sstable.get(key);
      if (tmp != "") {
	val = tmp;
      }
    }
  }
  
  return val;
}

// write_flush_queue takes in the flush_queue and writes the memtables from the
// queue into sstables on disk.
void DB::write_flush_queue() {
  while (this->m_running) {
    this->m_flush_mutex.lock();

    // create sstables for the oldest entries in the queue
    for (auto it = this->m_flush_queue.rbegin();
	 it != this->m_flush_queue.rend(); ++it) {
      // create new sstable
      auto sstable = SSTable();
      sstable.write_map(it->get_keymap());

      this->m_ss_mutex.lock();
      this->m_sstables.insert(this->m_sstables.begin(), sstable);
      this->m_ss_mutex.unlock();
    }

    this->m_flush_mutex.unlock();

    // sleep for 100 microseconds
    usleep(100);
  }
}

// flush_current_memtable resets the current memtable instance and also
// appends the latest memtable to the beginning of the flush_queue.
void DB::flush_current_memtable() {
  this->m_flush_mutex.lock();
  this->m_flush_queue.insert(this->m_flush_queue.begin(), this->m_memtable);
  this->m_flush_mutex.unlock();

  this->m_memtable_mutex.lock();
  this->m_memtable = Memtable();
  this->m_memtable_mutex.unlock();
}

// Put creates a entry in the database. It firstly checks that the memtable
// is not too big. After that it writes that vaue into the memtable.
void DB::put(const std::string& key, const std::string& value) {
  // 10 mb
  if (this->m_memtable.get_size() >= 10*1024*1024) {
    // after this the new values will be placed into the new memory table
    // since flush_current_memtable updates it.
    this->flush_current_memtable();
  }

  this->m_memtable_mutex.lock();
  this->m_memtable.put(key, value);
  this->m_memtable_mutex.unlock();
}

// Delete a value. It really doesn't delete the value it only makes an
// indication that a given entry has been deleted. It will get properly
// deleted when sstable compaction happens.
void DB::del(const std::string& key) {
  if (this->m_memtable.get_size() >= 10*1024*1024) {
    // after this the new values will be placed into the new memory table
    // since flush_current_memtable updates it.
    this->flush_current_memtable();
  }

  this->m_memtable_mutex.lock();
  this->m_memtable.put(key, "\00");
  this->m_memtable_mutex.unlock();
}

// start() starts running the database service and it also handles writing flush
// queue into sstables in a 'flush_thread'.
void DB::start() {
  this->m_running = true;

  std::thread flush_thread([this] { this->write_flush_queue();} );
}
