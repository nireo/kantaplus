#include "db.hpp"
#include "sstable.hpp"
#include <iostream>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

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

void DB::write_flush_queue() {
  while (this->m_running) {
    this->m_flush_mutex.lock();

    this->m_flush_mutex.unlock();

    // sleep for 100 microseconds
    usleep(100);
  }
}

void DB::flush_current_memtable() {
  this->m_flush_mutex.lock();
  this->m_flush_queue.insert(this->m_flush_queue.begin(), this->m_memtable);
  this->m_flush_mutex.unlock();

  this->m_memtable_mutex.lock();
  this->m_memtable = Memtable();
  this->m_memtable_mutex.unlock();
}

void DB::put(const std::string& key, const std::string& value) {
  // TODO: check that the size isn't too big
  this->m_memtable_mutex.lock();
  this->m_memtable.put(key, value);
  this->m_memtable_mutex.unlock();
}

void DB::del(const std::string& key) {
  // TODO: check that the size isn't too big
  this->m_memtable_mutex.lock();
  this->m_memtable.put(key, "\00");
  this->m_memtable_mutex.unlock();
}

void DB::start() {
  this->m_running = true;

  std::thread flush_thread([this] { this->write_flush_queue();} );
}
