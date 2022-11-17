#include <iostream>
#include "db/filename.h"

int main() {
    const std::string dbname = "test_db";
    const uint64_t number = 1986;

    std::cout << "LogFileName:" << leveldb::LogFileName(dbname, number) << std::endl;
    std::cout << "TableFileName:" << leveldb::TableFileName(dbname, number) << std::endl;
    std::cout << "SSTTableFileName:" << leveldb::SSTTableFileName(dbname, number) << std::endl;
    std::cout << "DescriptorFileName:" << leveldb::DescriptorFileName(dbname, number) << std::endl;
    std::cout << "TempFileName:" << leveldb::TempFileName(dbname, number) << std::endl;

    std::cout << "CurrentFileName:" << leveldb::CurrentFileName(dbname) << std::endl;
    std::cout << "LockFileName:" << leveldb::LockFileName(dbname) << std::endl;
    std::cout << "InfoLogFileName:" << leveldb::InfoLogFileName(dbname) << std::endl;
    std::cout << "OldInfoLogFileName:" << leveldb::OldInfoLogFileName(dbname) << std::endl;

    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
