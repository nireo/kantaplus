#include "db.hpp"
#include "memtable.hpp"
#include "rbtree.hpp"
#include <bits/stdint-intn.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <string>

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
  EXPECT_EQ(stat(memtable.get_log_path().c_str(), &buffer), 0);
}

TEST(MemtableTest, ByteConversions) {
  KVPair pair = {.key = "hello", .value = "world"};

  auto as_bytes = Memtable::to_bytes(pair);

  KVPair *test_pair = new KVPair();
  int status = Memtable::from_bytes(as_bytes, test_pair);

  // was successful?
  EXPECT_EQ(status, 0);

  EXPECT_STREQ(pair.key.c_str(), test_pair->key.c_str());
  EXPECT_STREQ(pair.value.c_str(), test_pair->value.c_str());
}

TEST(DatabaseTest, BasicOperations) {
  DB db = DB();
  EXPECT_STREQ(db.get("doesntexist").c_str(), "");

  // place value into the memtable
  db.put("hello", "world");
  EXPECT_STREQ(db.get("hello").c_str(), "world");
}

TEST(DatabaseTest, FolderCreated) {
  DB db = DB("./testdatabase");
  struct stat buffer;
  EXPECT_EQ(stat("./testdatabase", &buffer), 0);
}

TEST(DatabaseTest, ValueIsDeleted) {
	DB db = DB();
	db.put("hello", "world");

	// we can normally get the file
  EXPECT_STREQ(db.get("hello").c_str(), "world");

  // now we delete the value and it should equal to ""
  db.del("hello");
  EXPECT_STREQ(db.get("hello").c_str(), "");
}

TEST(DatabaseTest, QueueIsConcatenated) {
	DB db = DB();
	// 1000 bytes size
	db.set_maximum_size((int64_t)1000);
	for (int i = 0; i < 100; ++i) {
		db.put("key-"+std::to_string(i), "value-"+std::to_string(i));
	}

	// the amount of items in the flush queue should be less than 0.
	EXPECT_NE(db.get_flush_queue_size(), 0);
}
