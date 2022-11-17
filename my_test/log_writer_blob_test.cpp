#include <iostream>
#include "db/log_writer.h"
#include "leveldb/env.h"
#include "util/crc32c.h"
#include "util/coding.h"

int main() {
    std::string file_name("log_writer_blob.data");

    leveldb::WritableFile* file;
    leveldb::Status s = leveldb::Env::Default()->NewWritableFile(
            file_name,
            &file);

    leveldb::log::Writer writer(file);

    std::string data(leveldb::log::kBlockSize - leveldb::log::kHeaderSize - 10, 'a');
    s = writer.AddRecord(data);//字符串长度32751 = 0x7fef
    std::cout << s.ToString() << std::endl;

    data.assign("HelloWorld");
    s = writer.AddRecord(data);

    delete file;

    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
