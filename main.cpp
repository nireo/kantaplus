#include <iostream>
#include "memtable.hpp"

// this is the testing function
int main() {
    KVPair pair{
        .key = "hello",
        .value = "world"
    };

    auto bytes = Memtable::to_bytes(pair);

    auto *test_pair = new KVPair();
    int successful = Memtable::from_bytes(bytes, test_pair);
    if (successful) {
        std::cout << "from bytes unsuccessful" << "\n";
    }

    if (test_pair->key == pair.key)
        std::cout << "key working" << "\n";
    if (test_pair->value == pair.value)
        std::cout << "value working" << "\n";

    return EXIT_SUCCESS;
}
