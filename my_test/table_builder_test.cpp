#include <unistd.h>
#include <iostream>
#include "leveldb/env.h"
#include "leveldb/table_builder.h"

int main() {
    leveldb::Options options;
    options.block_restart_interval = 4;

    std::string file_name("table_builder.data");

    leveldb::WritableFile* file;
    leveldb::Status s = leveldb::Env::Default()->NewWritableFile(
            file_name,
            &file);

    leveldb::TableBuilder table_builder(options, file);
    table_builder.Add("confuse", "value");
    table_builder.Add("contend", "value");
    table_builder.Add("cope", "value");
    table_builder.Add("copy", "value");
    table_builder.Add("corn", "value");

    //flush后的文件
    //00000000: 0007 0563 6f6e 6675 7365 7661 6c75 6503  ...confusevalue.
    //00000010: 0405 7465 6e64 7661 6c75 6502 0205 7065  ..tendvalue...pe
    //00000020: 7661 6c75 6503 0105 7976 616c 7565 0004  value...yvalue..
    //00000030: 0563 6f72 6e76 616c 7565 0000 0000 2e00  .cornvalue......
    //00000040: 0000 0200 0000 00a7 ddaf 02              ...........
    //文件70 bytes，为block_contents
    //00 为CompressionType
    //a7dd af02为crc
    leveldb::Status status = table_builder.Finish();
    std::cout << status.ToString() << std::endl;

    std::cout << table_builder.NumEntries() << std::endl;
    std::cout << table_builder.FileSize() << std::endl;

    //close后的文件
    //00000000: 0007 0563 6f6e 6675 7365 7661 6c75 6503  ...confusevalue.
    //00000010: 0405 7465 6e64 7661 6c75 6502 0205 7065  ..tendvalue...pe
    //00000020: 7661 6c75 6503 0105 7976 616c 7565 0004  value...yvalue..
    //00000030: 0563 6f72 6e76 616c 7565 0000 0000 2e00  .cornvalue......
    //00000040: 0000 0200 0000 00a7 ddaf 0200 0000 0001  ................
    //00000050: 0000 0000 c0f2 a1b0 0001 0264 0046 0000  ...........d.F..
    //00000060: 0000 0100 0000 0032 6ceb 604b 0858 0e00  .......2l.`K.X..
    //00000070: 0000 0000 0000 0000 0000 0000 0000 0000  ................
    //00000080: 0000 0000 0000 0000 0000 0000 0000 0000  ................
    //00000090: 0000 0057 fb80 8b24 7547 db              ...W...$uG.`
    //close后新增数据依次为:
    //1. meta_index_block(offset=75, size=8)
    //未Add数据
    //因此block_contents: 00 0000 0001 0000 00
    //type && crc: 00 c0f2 a1b0
    //
    //2. index_block(offset=88, size=14)
    //Add的数据为:key=d value=|varint64(0) |varint64(70)  | ->0046
    //因此block_contents: 0001 0264 0046 0000 0000 0100 0000
    //type && crc: 0032 6ceb 60
    //
    //3. footer
    //metaindex_handle: |varint64(75)  |varint64(8)  | -> 4b08
    //index_handle: |varint64(88)  |varint64(14)  | -> 580e
    //36个00用于补全
    //magic: 57 fb80 8b24 7547 db
    file->Close();
    delete file;

    return 0;
}
