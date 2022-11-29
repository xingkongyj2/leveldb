// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

/**
 * iter就是memtable的迭代器。
 * meta保存新sstable的元信息。
 */
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  //构建sstable的文件名
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    //创建一个文件，用file来操作。
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    //将根据file创建一个TableBuilder类，用来构造sstable。
    //简单理解：将文件和TableBuilder类绑定。
    TableBuilder* builder = new TableBuilder(options, file);
    //sstable的最小key
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    //遍历meetable的数据，将数据一个个写入到sstable。
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    //遍历结束的时候，key肯定是一个最大key。
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    //sstable构建完成，并且写入到文件中。
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      // 验证表是否可用
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    //构建sstable出问题时，删除文件。
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
