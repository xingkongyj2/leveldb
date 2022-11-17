#include <iostream>
#include <vector>
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

int main() {
    const leveldb::FilterPolicy* bloom_filter = leveldb::NewBloomFilterPolicy(10);

    {
        std::string dst;
        const leveldb::Slice keys[2] = {"hello", "world"};
        bloom_filter->CreateFilter(keys, 2, &dst);

        std::cout << bloom_filter->KeyMayMatch(keys[0], dst) << std::endl;
        std::cout << bloom_filter->KeyMayMatch(keys[1], dst) << std::endl;
        std::cout << bloom_filter->KeyMayMatch("ufo exists?", dst) << std::endl;
        std::cout << bloom_filter->KeyMayMatch("nullptr", dst) << std::endl;
    }

    {
        std::string dst;
        std::vector<leveldb::Slice> keys;
        for (int i = 0; i < 10000; ++i) {
            keys.push_back(std::string(i, 'a'));
        }
        bloom_filter->CreateFilter(&keys[0], int(keys.size()), &dst);

        int fail_count = 0;
        for (int i = 0; i < 10000; ++i) {
            if (bloom_filter->KeyMayMatch(std::string(i, 'b'), dst)) {
                fail_count++;
            }
        }

        std::cout << "try 10000 times, fail:" << fail_count << std::endl;
    }

    {
        std::string dst;
        const leveldb::Slice keys[3] = {"a", "b", "c"};
        bloom_filter->CreateFilter(keys, 3, &dst);
        std::cout << dst;
    }

    delete bloom_filter;

    return 0;
}
