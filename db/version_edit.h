// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {
  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;
  int allowed_seeks;  // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;    // File size in bytes
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
};

/**
 * 相对当前版本的增量信息，同样包括了几个功能：
 *     1）与文件序号相关的记录：Version在合并时，新生成的文件会使用统一的序号，
 *     所以新文件的序号也会发生变化。这些文件序号变化也需要做为更改的一部分记录下来，
 *     各种has_xxx指示符就是说明这个序号是否发生了变化。而序号的更改，并不是累加到展现到Version上，
 *     而是直接作用于VersionSet。
 *
 *     2）压缩指针compact_pointers_：由于SSTable文件数量可能庞大，合并操作并不是一次性完成的，
 *     而且过多的压缩Compaction也会占用机器CPU资源，因此，压缩是阶段式进行的。
 *     压缩指针compact_pointers_记录在版本增量VersionEdit中，并不记录在版本Version中，
 *     每个Level下一次合并的位置最终是记录在VersionSet中的compact_pointer_。
 *
 *     每一个Version需要合并的时候，都会经过一定的计算得到每个Level下一次合并的位置，
 *     在合并操作Apply()时，直接使用压缩指针compact_pointers_中记录的Level作为索引，
 *     因为是版本的Apply，这意味着总是后面的一个版本增量VersionEdit去覆盖前面一个版本增量VersionEdit的合并位置，
 *     因此可直接赋值覆盖VersionSet中的compact_pointer_。
 *
 *     3）与文件变化有关的记录：新增文件信息new_files_和删除文件信息deleted_files_，
 *     这两个成员变量都是一个std::pair<>对象，表示哪个level新增和删除的文件信息。
 */
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file, uint64_t file_size,
               const InternalKey& smallest, const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector<std::pair<int, FileMetaData>> new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
