#include "db.hpp"
#include "sstable.hpp"
#include <iostream>
#include <sys/stat.h>

DB::DB(const std::string& directory) {
  int ok;
  ok = mkdir(directory.c_str(), 0777);
  if (!ok) {
    std::cout << "could not start database" << "\n";
    m_ok = false;
    return;
  } else {
    m_ok = true;
    return;
  }

  m_directory = directory;
  m_memtable = Memtable();
  m_sstables = std::vector<SSTable>();
  m_flush_queue = std::vector<Memtable>();
}
