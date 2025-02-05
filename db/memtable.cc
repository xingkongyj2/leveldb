// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

/**
 * 给出LookupKey的地址，解析出internal key
 */
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  //跳过length的字节，获取internal key的起始地址，length最多占5个字节。
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

/**
 * operator()负责解析出 internal key，交给 comparator
 */
int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  //内部键被编码为以长度为前缀的字符串。
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

/**
 * Add过程的代码就是组装memtable key，然后调用SkipList接口写入。
 */
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  tag          : uint64((sequence << 8) | type)
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  //InternalKey长度 = UserKey长度 + 8bytes(存储SequenceNumber + ValueType)
  size_t internal_key_size = key_size + 8;
  //用户写入的key value在内部实现里增加了sequence type
  //而到了MemTable实际按照以下格式组成一段buffer
  //|encode(internal_key_size)  |key  |sequence  type  |encode(value_size)  |value  |
  //这里先计算大小，申请对应大小的buffer
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  //append sequence_number && type
  //64bits = 8bytes，前7个bytes存储s，最后一个bytes存储type.
  //这里8bytes对应前面 internal_key_size = key_size + 8
  //也是Fixed64而不是Varint64的原因
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  //写入table_的buffer包含了key/value及附属信息
  table_.Insert(buf);
}

/**
 * Get通过SkipList::Iterator::Seek接口获取第一个 >=查询key 的Node
 * 传入的第一个参数是LookupKey包含了 userkey，
 * 同时指定了一个较大的SequenceNumber s（具体多么大我们后续分解），
 * 而根据InternalKeyComparator的定义，返回值有两种情况：
 *     1.如果该 userkey 存在，返回小于 s 的最大 sequence number 的 Node.
 *     2.如果 userkey 不存在，返回第一个 > userkey 的 Node.
 */
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    //解析出internal_key的长度存储到key_length
    //key_ptr指向internal_key
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    //Seek返回的是第一个>=key的Node(>= <=> InternalKeyComparator::Compare)
    //因此先判断下userkey是否相等
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      // tag = (s << 8) | type
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      //type存储在最后一个字节
      switch (static_cast<ValueType>(tag & 0xff)) {
        //因为只有新增和删除，所以一个key只有两种状态。
        //取数情况：
        //    key不存在：返回false
        //    key存在：
        //        key是写入状态：正常返回
        //        key是删除状态：返回空
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
