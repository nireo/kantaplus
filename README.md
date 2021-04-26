# kantaplus

A C++ implementation of [kantadb](https://github.com/nireo/kantadb). Not really a serious project I just wanted to practice C++.

## About

This project is designed to be en embeddable database to be used as a library. It is based on LSM-trees.

## Running tests

```
cmake -S . -B build
cmake --build build
cd build && ctest
```
