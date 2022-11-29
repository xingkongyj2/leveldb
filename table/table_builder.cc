// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

/**
 * TableBuilder被用来生成sstable，实现上都封装到了class leveldb::TableBuilder::Rep
 */
struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        //todo：会用&options作为参数构造一个临时对象赋值给data_block？
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  //文件信息
  WritableFile* file;
  //所有data_block的偏移量。
  uint64_t offset;
  Status status;
  //data block，index block都采用相同的格式，通过BlockBuilder完成。
  //sstable的blobk：
  //    data_block
  //    filter_block（单独的格式）
  //    mata_index_block（最后写入的时候临时生成）
  //    index_block
  //    footer（最后写入的时候临时生成）
  //sstable中的数据块
  BlockBuilder data_block;
  //sstable中data_block的index_block
  BlockBuilder index_block;
  std::string last_key;
  //一个kv一个entry，entry的个数。
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  //sstable中的过滤器
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

/**
 * 生成sstable的时候也是一个kv，一个kv的写入。
 */
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  //每次向data_block插入kv，当block达到了上限，将这个block写入到文件后，就将pending_index_entry置为true。
  //同时，代表需要向index_block块新增一个entity。这个entity指向这个块
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    //todo：为什么？
    //计算一个满足：>r->last_key && <= key 的字符串，存储到r->last_key
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    //pending_handle记录的是上个block的offset及大小
    r->pending_handle.EncodeTo(&handle_encoding);
    //index_block中entity的key为所指向的data_block中最大的key，value为这个data_block的位置和大小。
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  //将当前key加入到过滤器块中。
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  //当data_bloc到达了上限的时候，就Flush，将当前data_block写到文件中。
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

/**
 * Flush主要是将r->data_block更新到文件，记录该 data block的offset及大小，
 * 等待下次Add or Finish时写入(原因参考Add)。
 */
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  //写入r->data_block到文件中。
  //更新pending_handle: 记录一个块在sstable中的位置，
  //    size为这个data_block的大小，offset为这个data_block的起始地址。
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    //因为写入文件后，文件的数据只是还在内核的缓冲区。
    //调用底层刷新函数，将数据落到磁盘中。
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    //将当前块的offset写入过滤器。
    r->filter_block->StartBlock(r->offset);
  }
}

/**
 * 其实就是从 block 取出数据，判断是否需要压缩，将最终结果调用WriteRawBlock。
 *
 * 判断是否压缩：
 *     1.如果设置了kNoCompression，那么一定不压缩
 *     2.如果设置了kSnappyCompression，那么尝试 snappy 压缩，
 *       如果压缩后的大小小于原来的 87.5%，那么使用压缩后的值，否则也不压缩
 *
 * N个 data blocks, 1个 index block，1个 meta_index block，都使用这种方式写入，
 * 也就是都采用BlockBuilder构造的数据组织格式，filter block的数据格式由FilterBlockBuilder构造。
 */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  //raw包含了这个data_block的全部信息：数据和restart
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;
    //压缩数据
    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

/**
 * 依次写入block_contents、1bytes的compression_type、4bytes的的crc，
 * 其中后5个字节称为 BlockTrailer，大小定义为：
 *     static const size_t kBlockTrailerSize = 5; //1-byte type + 32-bit crc。
 *
 * 对应格式图里右上角部分，所有的 block，例如 data block/filter block/meta index block/index block，
 * 都按照|block_contents |compression_type |crc |这种格式组织，区别是 block_contents 格式不同。
 *
 * handle为输出变量，size为这个data_block的大小，offset为这个data_block的起始地址。
 */
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  //r->offset起始就是当前sstable的大小(byte)
  //handle记录当前block的起始偏移位置和大小
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  //直接将这个data_block追加到文件末尾。
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    //追加每个block的尾部压缩信息
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    //todo:crc原理是什么？
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      //更新offset
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

/**
 * data_block构建完成之后。使用Finish写入：filter block、metaindex block、index block
 * 、footer等其它四个字段。
 */
Status TableBuilder::Finish() {
  Rep* r = rep_;
  //将最后一个data_block写入到sstable
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // filter block写入sstable
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // 写入index of filter block，这里称为meta_index_block
  if (ok()) {
    //meta_index_block只写入一条数据
    //key: filter.$filter_name
    //value: filter_block的起始位置和大小
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
