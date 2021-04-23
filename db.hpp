#ifndef KANTAPLUS_DB_HPP
#define KANTAPLUS_DB_HPP

#include "memtable.hpp"
#include "sstable.hpp"
#include <string>
#include <vector>

class DB {
public:
  std::string get(const std::string& key);
  void put(const std::string&, const std::string& key);
  void del(const std::string& key);

  // write the contents of the memtable flush queue into sstables
  void write_flush_queue();

  void start() const;

  explicit DB(const std::string& directory);
private:
  std::vector<SSTable> m_sstables;
  std::vector<Memtable> m_flush_queue;
  Memtable m_memtable;
  std::string m_directory;

  // a identifier indicating if the database has been setup successfully.
  bool m_ok;
};

#endif // KANTAPLUS_DB_HPP
