#include <iostream>
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "leveldb/slice.h"
#include "leveldb/env.h"

int main() {
    leveldb::SequentialFile* file;
    //MANIFEST files
    leveldb::Status status = leveldb::Env::Default()->NewSequentialFile("./data/test_table.db/MANIFEST-000004", &file);
    std::cout << status.ToString() << std::endl;

    leveldb::log::Reader reader(file, NULL, true/*checksum*/, 0/*initial_offset*/);
    // Read all the records and add to a memtable
    std::string scratch;
    leveldb::Slice record;
    while (reader.ReadRecord(&record, &scratch) && status.ok()) {
        leveldb::VersionEdit edit;
        edit.DecodeFrom(record);
        std::cout << edit.DebugString() << std::endl;
    }
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
