// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}


/**
 * 注意：*(ptr++)中++是最后生效的，效果就是赋值后指针往后++
 */
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  //v<128
  if (v < (1 << 7)) {
    *(ptr++) = v;
  }
  //128<=v<16384
  else if (v < (1 << 14)) {
    //v | B = 1111000011110000 | 10000000 = 1111000011110000，保存在char*中，从后8位截断，得到：1,1110000。
    //B的低7位是0，可以保留v的低7位，而最高位1是标志位，表示数据还没有结束。
    //效果就是：v的最低7位保存在prt指向的字节，并且将最高位置1，表示数据没有结束，并且ptr后移。
    //将v的低7位保存后，将低7位右移丢弃，将剩下的数据保存在ptr中。
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  }
  else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  }
  else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  }
  else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  //返回地址ptr是当前编码后空间地址
  return reinterpret_cast<char*>(ptr);
}

/**
 * 说明：uint32_t v是数据保存在计算机中的最终形式，表现形式可能是有符号的，可能是字符串，可能是汉字。
 * 编码一位32位无符号int需要buf 5字节空间，返回地址ptr是当前编码后空间地址，prt-buf就是编码数据的长度。
 * 注意：这种重新对数据编码的方式并不于string的内存分配方式冲突，原本放入string中要4个字节，现在重新编码后就只需要2个字节了。
 * [图解 C++ 中 std::string 的内存布局 - 知乎](https://zhuanlan.zhihu.com/p/510507837)
 */
void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
  while (v >= B) {
    *(ptr++) = v | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<uint8_t>(v);
  return reinterpret_cast<char*>(ptr);
}

void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

/**
 * 小于128，直接就是1Byte；大于128，就看能右移几次，每右移一次代表一个字节。
 * 例子：
 *     11,1100001,1110000一共16位，因为每个字节只能7位能表示数据，所以需要3个字节来保存。
 *     字节情况：[0,0000011][1,1100001][1,1110000]，每个字节的最高位表示数字是否结束，0-未结束，1-结束。
 *     解析过程：从一个低地址开始读取7位，如果这个字节的最后位为1，就继续读取下一个字节。直到读取的当前字节最高位为0。
 *             读取1110000，发现最高位为1，继续读取下一字节的1100001，发现最高位为1，继续读取下一字节的0000011，发现最高位为0，读取结束。
 * 说明：高地址->低地址
 */
int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    //get one byte value
    uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
    //input指针一直在递增！
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      //最后返回的input指针的当前位置，已经不是开始位置了。
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

/**
 *
 */
bool GetVarint64(Slice* input, uint64_t* value) {
  //Footer起始地址
  const char* p = input->data();
  //Footer结束地址
  const char* limit = p + input->size();
  //value已经解析出第一个值，q指针是下一个值的地址
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    //用下一个值的开始地址和剩余值大小，重新生成一个input
    *input = Slice(q, limit - q);
    return true;
  }
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  if (GetVarint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb
