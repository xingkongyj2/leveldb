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
    // 打开sstable文件
    //RandomAccessFile指向PosixRandomAccessFile或者PosixMmapReadableFile
    leveldb::RandomAccessFile* file = nullptr;
    // PosixEnv类中的NewRandomAccessFile
    // 打开的sstable文件放入file中
    leveldb::Status status = leveldb::Env::Default()->NewRandomAccessFile(
            file_path,
            &file);
    std::cout << "NewRandomAccessFile status:" << status.ToString() << std::endl;

    //New Table
    struct stat file_stat;
    // table为出参，sstable信息保存到table的Rep
    stat(file_path.c_str(), &file_stat);
    status = leveldb::Table::Open(
            leveldb::Options(),
            file,
            file_stat.st_size,
            &table);
    std::cout << "leveldb::Table::Open status:" << status.ToString() << std::endl;

    // iter指向的是TwoLevelIterator类
    // table的迭代器其实就是：构建index_block的迭代器，根据index_block迭代器遍历index_block的kv。
    //     遍历index_block的kv时，根据value建立每个data_block的迭代器。从而遍历每个data_block
    //     中的kv。

    // 而迭代器其实就是：1.block的整理信息：起始地址，restart_offset等。2.记录当前kv的数据。
    leveldb::Iterator* iter = table->NewIterator(leveldb::ReadOptions());
    // index_block的迭代器指向index_block中的第一个kv数据。
    iter->SeekToFirst();

    while (iter->Valid()) {
        //iter->key()：读取当前data_block指向的数据。
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
