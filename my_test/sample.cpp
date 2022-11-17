#include <assert.h>
#include <iostream>
#include "leveldb/db.h"

int main() {
    leveldb::DB* db = NULL;
    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::Status status = leveldb::DB::Open(options, "./data/my_sample.db", &db);
    std::cout << status.ToString() << std::endl;

    std::string key = "age";
    std::string this_year_value = "21";
    std::string next_year_value = "22";
    std::string db_value;

    //写入key, value
    status = db->Put(leveldb::WriteOptions(), key, this_year_value);
    assert(status.ok());

    //记录snapshot
    const leveldb::Snapshot* snapshot = db->GetSnapshot();

    //写入相同key，不同value
    status = db->Put(leveldb::WriteOptions(), key, next_year_value);
    assert(status.ok());
    //读取当前value
    status = db->Get(leveldb::ReadOptions(), key, &db_value);
    assert(status.ok());
    std::cout << "current: " << key << " -> " << db_value << std::endl;

    leveldb::ReadOptions read_options;
    read_options.snapshot = snapshot;
    //读取snapshot的value
    status = db->Get(read_options, key, &db_value);
    assert(status.ok());
    std::cout << "snapshot: " << key << " -> " << db_value << std::endl;

    db->Put(leveldb::WriteOptions(), "name", "Jeff Dean");
    db->Put(leveldb::WriteOptions(), "company", "Google");
    status = db->Get(read_options, "name", &db_value);
    std::cout << "Get snapshot: " << "name" << " status:" << status.ToString() << std::endl;

    //遍历
    {
        leveldb::Iterator* iter = db->NewIterator(leveldb::ReadOptions());
        iter->SeekToFirst();
        std::cout << "scan current:" << std::endl;
        while (iter->Valid()) {
            std::cout << iter->key().ToString() << " -> " << iter->value().ToString() << std::endl;
            iter->Next();
        }
        delete iter;
    }
    {
        leveldb::Iterator* iter = db->NewIterator(read_options);
        iter->SeekToFirst();
        std::cout << "scan snapshot:" << std::endl;
        while (iter->Valid()) {
            std::cout << iter->key().ToString() << " -> " << iter->value().ToString() << std::endl;
            iter->Next();
        }
        delete iter;
    }

    db->ReleaseSnapshot(snapshot);
    delete db;

    return 0;
}
