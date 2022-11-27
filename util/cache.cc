// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"
#include <iostream>

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
/**
 * 查找entry的过程不变，确定好了entry是属于那个block之后，读取block的时候缓存才起作用。
 * 一个LRUHandle保存一个block或者Table。
 * 1.Block：key(cache_id+offset): value(整个Block对象)
 * 2.Table：key(file_number):TableAndFile(结构体)
 */
struct LRUHandle {
  // 值：是整个 Block 对象
  void* value;
  //当refs降为0时的清理函数
  void (*deleter)(const Slice&, void* value);
  // 哈希值相同的下一个节点
  LRUHandle* next_hash;
  //LRU链表双向指针（用在维护lru_或者in_use_双链表中）
  LRUHandle* next;
  //LRU链表双向指针（用在维护lru_或者in_use_双链表中）
  LRUHandle* prev;
  // 缓存项的大小：这个缓存项所占的大小(cache块默认是1)（capacity_总大小为15）。
  size_t charge;  // TODO(opt): Only allow uint32_t?
  // 键的长度
  size_t key_length;
  // 当前项是否在缓存中
  bool in_cache;     // Whether entry is in the cache.
  // 引用计数，用于删除数据
  // refs==0：节点将被销毁，refs==1：节点在lru_中，refs==2：节点在in_use_中
  uint32_t refs;     // References, including cache reference, if present.
  // key 对应的hash值
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
  // 键值
  char key_data[1];  // Beginning of key

  Slice key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
/**
 * 桶的个数初始化大小为4，随着元素增加动态修改，使用数组实现。
 * 同一个 bucket 里，使用链表存储全部的 LRUHandle*，最新插入的数据排在链表尾部。
 * 核心函数是FindPointer。
 */
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  /**
   * hashTable节点的存储是单链表。
   */
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    //将新的节点指向旧节点的next
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    //h是在这里插到table的。
    // ptr的值：
    //     1.ptr!=nullptr：说明找到了目标key，ptr是 目标LRUHandle指针的地址。
    //         直接将新节点的地址赋值给*ptr，就能让旧LRUHandle的上一个节点指向新的节点。
    //     2.ptr==nullptr：与上同理。
    *ptr = h;
    //新插入一个LRUHandle
    if (old == nullptr) {
      ++elems_;
      //超过阀值，扩容。
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  // LRUHandle个数阀值，超过阀值就要扩容。
  uint32_t length_;
  // 当前LRUTable中LRUHandle的个数。
  uint32_t elems_;
  // LRUHandle*数组的地址。
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // 如果某个LRUHandle*存在相同的hash&&key值，则返回 指向该LRUHandle指针的地址，
  //     即：该LRUHandle前一个LRUHandle的next_hash的地址。
  // 如果不存在这样的LRUHandle*，则返回指向该bucket的最后一个LRUHandle*的next_hash的二级指针，其值为nullptr。
  // 返回next_hash地址的作用是可以直接修改该值，因此起到修改链表的作用。
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    //先查找处于哪个桶
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    //next_hash 查找该桶，直到满足以下条件之一：
    //*ptr == nullptr
    //某个LRUHandle* hash和key的值都与目标值相同
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    //返回符合条件的LRUHandle**
    return ptr;
  }

  /**
   * hashtable扩容。
   */
  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
/**
 * 实现了 LRU 的所有功能。一个LRUCache同一时刻只能被一个进程访问。读写都会加锁。
 * 包含：
 *     1.LRUHandle：LRUNode，节点
 *     2.HandleTable：哈希数据结构，提供了Lookup/Insert/Remove接口，实现数据的查询、更新、删除。
 */
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  // 缓存容量默认15
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  // 缓存容量默认15
  size_t capacity_;

  // mutex_ protects the following state.
  // 包含缓存的锁
  mutable port::Mutex mutex_;
  // 当前使用了多少容量
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  // 缓存项链表
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  // 当前正在被使用的缓存项链表。头节点（不保存数据），方便增删
  LRUHandle in_use_ GUARDED_BY(mutex_);

  // 缓存的哈希表，快速查找缓存项。头节点（不保存数据），方便增删
  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  // 如果当前在lru_里，移动到in_use_里
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    // 先从链表中移除
    LRU_Remove(e);
    // 插入到in_use_
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  std::cout << "LRUHandle refs:" << e->refs
            << std::endl;
  e->refs--;
  // 销毁缓存项
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    //这个节点还在被使用。
    // No longer in use; move to lru_ list.
    // 重新移动到lru_里
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  // 加锁操作，使用分段缓存减少锁等待
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}


Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  //加锁了，所以同一时间，读写不能同时存在。
  MutexLock l(&mutex_);

  // 申请动态大小的LRUHandle内存，初始化该结构体
  // refs = 2:
  // 1. 返回的Cache::Handle*
  // 2. in_use_链表里记录
  // 因为LRUHandle实际上比分配的要大，所以重新reinterpret_cast
  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  // refs==0：节点将被销毁，refs==1：节点在lru_中，refs==2：节点在in_use_中
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    //e被使用
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    //添加到in_use_队列
    LRU_Append(&in_use_, e);
    usage_ += charge;
    // 如果是更新的，删除原有节点。
    // 插入到table_，如果 key&&hash 之前存在，
    //     那么HandleTable::Insert会返回原来的LRUHandle*对象指针，
    //     调用FinishErase清理进入状态1。
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  // 如果超过了容量限制，根据lru_按照lru策略淘汰
  while (usage_ > capacity_ && lru_.next != &lru_) {
    // lru_.next是最老的节点，首先淘汰
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
/**
 * 删除旧节点。新增节点的时候加了锁，所以旧节点一定refs==1。
 */
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

/**
 * 定义多个LRUCache，实现分段式锁。
 */
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    // capacity=16 kNumShards=16
    // per_shard=15
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    //一共有16个LRUCache，取hash的高4个字节，确定进入哪一个LRUCache。
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
