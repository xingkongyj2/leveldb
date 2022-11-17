#include <iostream>
#include "db/dbformat.h"

void test_internal_key() {
    {
        leveldb::InternalKey internal_key("name", 1234L, leveldb::kTypeValue);
        std::cout << internal_key.DebugString() << std::endl;
    }
    {
        leveldb::InternalKey internal_key("name", 1234L, leveldb::kTypeDeletion);
        std::cout << internal_key.DebugString() << std::endl;
    }
}

int main() {
    test_internal_key();

    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
