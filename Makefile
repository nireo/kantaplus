all:
	g++ -std=c++17 main.cpp memtable.cpp sstable.cpp -o output -g
