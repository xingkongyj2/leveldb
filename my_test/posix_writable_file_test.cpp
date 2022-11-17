#include <iostream>
#include "leveldb/env.h"

int main() {
    std::string file_name("posix_writable_file_test.data");

    leveldb::WritableFile* file;
    leveldb::Status s = leveldb::Env::Default()->NewWritableFile(
            file_name,
            &file);

    std::cout << s.ToString() << std::endl;
    file->Append("hello, world\n");
    file->Append("hello, go\n");
    file->Append("hello, programmer\n");
    file->Append(std::string("\x00\x00\x00\x00\x00\x00", 4));

    file->Flush();

    return 0;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
