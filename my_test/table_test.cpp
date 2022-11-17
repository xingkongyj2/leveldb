#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#include <iostream>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/table.h"

void scan_by_table_iterator() {
    leveldb::Table* table = nullptr;
    std::string file_path = "./data/test_table.db/000005.ldb";
    // std::string file_path = "./table_builder.data";

    //New RandomAccessFile
    leveldb::RandomAccessFile* file = nullptr;
    leveldb::Status status = leveldb::Env::Default()->NewRandomAccessFile(
            file_path,
            &file);
    std::cout << "NewRandomAccessFile status:" << status.ToString() << std::endl;

    //New Table
    struct stat file_stat;
    stat(file_path.c_str(), &file_stat);
    status = leveldb::Table::Open(
            leveldb::Options(),
            file,
            file_stat.st_size,
            &table);
    std::cout << "leveldb::Table::Open status:" << status.ToString() << std::endl;

    leveldb::Iterator* iter = table->NewIterator(leveldb::ReadOptions());
    iter->SeekToFirst();

    while (iter->Valid()) {
        std::cout << iter->key().ToString() << "->" << iter->value().ToString() << std::endl;
        iter->Next();
    }

    delete iter;
    delete file;
    delete table;
}

int main() {
    scan_by_table_iterator();

    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
