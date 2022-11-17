#include <iostream>
#include "leveldb/filter_policy.h"
#include "table/filter_block.h"

int main() {
    const leveldb::FilterPolicy* bloom_filter = leveldb::NewBloomFilterPolicy(10);
    leveldb::FilterBlockBuilder filter_block_builder(bloom_filter);

    filter_block_builder.StartBlock(0);
    //1000 1431 1109 0002 06
    filter_block_builder.AddKey("Hello");
    filter_block_builder.AddKey("World");
    filter_block_builder.StartBlock(3000);
    //2002 0043 8821 4404 06
    filter_block_builder.AddKey("Go");
    filter_block_builder.AddKey("Programmer");
    filter_block_builder.StartBlock(20000);
    //1a38 64d0 c001 8300 06
    filter_block_builder.AddKey("a");
    filter_block_builder.AddKey("b");
    filter_block_builder.AddKey("c");

    leveldb::Slice result = filter_block_builder.Finish();

    //00000000: 1000 1431 1109 0002 0620 0200 4388 2144  ...1..... ..C.!D
    //00000010: 0406 1a38 64d0 c001 8300 0600 0000 0009  ...8d...........
    //00000020: 0000 0012 0000 0012 0000 0012 0000 0012  ................
    //00000030: 0000 0012 0000 0012 0000 0012 0000 0012  ................
    //00000040: 0000 001b 0000 000b                      ........
    // std::cout << result.ToString();

    leveldb::FilterBlockReader filter_block_reader(bloom_filter, result);
    std::cout << filter_block_reader.KeyMayMatch(0, "Hello") << std::endl;//1
    std::cout << filter_block_reader.KeyMayMatch(0, "World") << std::endl;//1
    std::cout << filter_block_reader.KeyMayMatch(0, "Go") << std::endl;//0
    std::cout << filter_block_reader.KeyMayMatch(3000, "Go") << std::endl;//1
    std::cout << filter_block_reader.KeyMayMatch(20000, "b") << std::endl;//1
    std::cout << filter_block_reader.KeyMayMatch(20000, "d") << std::endl;//0

    delete bloom_filter;
}
