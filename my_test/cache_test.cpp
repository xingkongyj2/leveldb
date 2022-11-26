#include <iostream>
#include <vector>
#include "leveldb/cache.h"
#include "util/hash.h"

std::vector<std::string> keys;
std::vector<int> values;

void deleter(const leveldb::Slice& key, void* value) {
    keys.push_back(key.ToString());
    values.push_back(*static_cast<int*>(value));

    std::cout << "deleter key:" << *keys.rbegin()
        << " value:" << *values.rbegin()
        << std::endl;
}

void test_hash() {
    for (int i = 0; i < 26; ++i) {
        std::string key(1, 'a' + i);
        std::cout << "key:" << key
            << " hash:"
            << (leveldb::Hash(key.data(), key.size(), 0) >> 28) << std::endl;
    }
}

int main() {
    leveldb::Cache* cache = leveldb::NewLRUCache(16);

    std::vector<std::string> orignal_keys{"d", "m", "v"};
    std::vector<int> orignal_values{100, 101, 201};
    std::vector<leveldb::Cache::Handle*> handles;

    //新增的handle节点的ref实际上为2
    for (size_t i = 0; i < orignal_keys.size(); ++i) {
        // cache->Insert = ShardedLRUCache->Insert
         handles.push_back(cache->Insert(
                orignal_keys[i],
                static_cast<void*>(&orignal_values[i]),
                1,
                deleter));
         std::cout << "Insert key:" << orignal_keys[i]
             << " value:" << *static_cast<int*>(cache->Value(handles[i]))
             << std::endl;
    }

    //手动释放一次，所有节点ref为1了。
    for (size_t i = 0; i < handles.size(); ++i) {
        cache->Release(handles[i]);
    }

    //key为d的节点由于被引用了，所以ref=2，其他节点还是1。
    leveldb::Cache::Handle* handle = cache->Lookup("d");
    std::cout << "Lookup key:d value:"
        << *static_cast<int*>(cache->Value(handle)) << std::endl;
    //手动释放一次key为d的节点。现在所有节点的ref都是1了。
    cache->Release(handle);

    //持有handle直到现在释放
    // for (size_t i = 0; i < handles.size(); ++i) {
        // cache->Release(handles[i]);
    // }

    //delete的时候，析构函数会将他们全部删除。
    delete cache;

    return 0;
}
