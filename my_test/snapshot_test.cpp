#include <assert.h>
#include <iostream>
#include <vector>
#include "leveldb/db.h"
#include "db/snapshot.h"

int main() {
    leveldb::DB* db = NULL;
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status = leveldb::DB::Open(options, "./data/my_sample.db", &db);
    std::cout << status.ToString() << std::endl;

    std::string key = "age";
    std::string db_value;

    std::vector<const leveldb::Snapshot*> snapshots;
    //记录snapshot
    snapshots.push_back(db->GetSnapshot());
    std::cout << static_cast<const leveldb::SnapshotImpl*>(snapshots.back())->sequence_number() << std::endl;

    for (int i = 0; i < 10; ++i) {
        std::string value = std::string(1, '0' + i);
        status = db->Put(leveldb::WriteOptions(), key, value);
        snapshots.push_back(db->GetSnapshot());
        std::cout << "Put key:" << key << " value:" << value << std::endl;
    }

    //读取当前value
    status = db->Get(leveldb::ReadOptions(), key, &db_value);
    assert(status.ok());
    std::cout << "current: " << key << " value:" << db_value << std::endl;

    for (size_t i = 0; i < snapshots.size(); ++i) {
        leveldb::ReadOptions read_options;
        read_options.snapshot = snapshots[i];
        //读取snapshot的value
        status = db->Get(read_options, key, &db_value);
        std::cout << "snapshot: "
            << static_cast<const leveldb::SnapshotImpl*>(snapshots[i])->sequence_number()
            << " key:" << key << " value:" << db_value << " status:" << status.ToString()
            << std::endl;
    }

    // db->ReleaseSnapshot(snapshot);
    delete db;

    return 0;
}
