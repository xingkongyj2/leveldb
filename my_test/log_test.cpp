#include <iostream>
#include "util/logging.h"
#include "leveldb/slice.h"

int main() {
    {
        leveldb::Slice in = "0012345.log";
        uint64_t value = 0;
        bool parsed = leveldb::ConsumeDecimalNumber(&in, &value);
        std::cout << "parsed:" << parsed << " value:" << value << std::endl;
        std::cout << "in:" << in.ToString() << std::endl;
    }

    {
        leveldb::Slice in = "18446744073709551615.suffix";
        uint64_t value = 0;
        bool parsed = leveldb::ConsumeDecimalNumber(&in, &value);
        std::cout << "parsed:" << parsed << " value:" << value << std::endl;
        std::cout << "in:" << in.ToString() << std::endl;
    }

    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
