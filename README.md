# kantaplus

This is a key-value store inspired by [bitcask](https://riak.com/assets/bitcask-intro.pdf). It is not recommended for production use.

## Pros & Cons

Pros:
* Fast writes
* Low latency

Cons:
* Since key metadata is held in memory it is quite memory intensive
* Cannot hold billions of keys, since they use too much ram.

## Running tests

```
cmake -S . -B build
cmake --build build
cd build && ctest
```
