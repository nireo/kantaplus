#include "memtable.hpp"
#include <gtest/gtest.h>
#include "rbtree.hpp"
#include <sys/stat.h>

// Demonstrate some basic assertions.
TEST(MemtableTest, BasicOperations) {

  Memtable memtable = Memtable();
  EXPECT_STREQ(memtable.get("doesntexist").c_str(), "");

  // place value into the memtable
  memtable.put("hello", "world");
  EXPECT_STREQ(memtable.get("hello").c_str(), "world");
}

TEST(MemtableTest, LogFile) {
  Memtable memtable = Memtable();
  struct stat buffer;   
  EXPECT_EQ(stat (memtable.get_log_path().c_str(), &buffer), 0); 
}

TEST(MemtableTest, ByteConversions) {
  KVPair pair = {
    .key = "hello",
    .value = "world"
  };

  auto as_bytes = Memtable::to_bytes(pair);

  KVPair *test_pair = new KVPair();
  int status = Memtable::from_bytes(as_bytes, test_pair);

  // was successful?
  EXPECT_EQ(status, 0);

  EXPECT_STREQ(pair.key.c_str(), test_pair->key.c_str());
  EXPECT_STREQ(pair.value.c_str(), test_pair->value.c_str());
}
