#include <iostream>

#include "db/log_format.h"
#include "util/crc32c.h"

int main() {
    std::string type(1, leveldb::log::kFullType);
    std::string value("hello world");

    uint32_t type_crc = leveldb::crc32c::Value(type.c_str(), type.size());
    // 2035593791
    std::cout << leveldb::crc32c::Extend(type_crc, value.c_str(), value.size()) << std::endl;

    std::string buffer = type + value;
    // 2035593791
    std::cout << leveldb::crc32c::Value(buffer.c_str(), buffer.size()) << std::endl;

    std::string value1("hello");
    std::string value2(" world");
    uint32_t value1_crc = leveldb::crc32c::Value(value1.c_str(), value1.size());
    // 3381945770
    std::cout << leveldb::crc32c::Extend(value1_crc, value2.c_str(), value2.size()) << std::endl;

    std::string value_all = value1 + value2;
    // 3381945770
    std::cout << leveldb::crc32c::Value(value_all.c_str(), value_all.size()) << std::endl;

    return 0;
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
