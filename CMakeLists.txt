cmake_minimum_required(VERSION 3.14)
project(kantaplus)

# GoogleTest requires at least C++11
set(CMAKE_CXX_STANDARD 20)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)

FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
	tests
	tests.cpp
	memtable.cpp
	db.cpp
	sstable.cpp
)

target_link_libraries(
	tests
	gtest_main
)

include(GoogleTest)
gtest_discover_tests(tests)
