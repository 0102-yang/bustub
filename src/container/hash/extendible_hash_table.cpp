//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(const size_t bucket_size)
    : global_depth_(0), num_buckets_(1), bucket_size_(bucket_size), dir_{std::make_shared<Bucket>(bucket_size, 0)} {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::lock_guard lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(const int dir_index) const -> int {
  std::lock_guard lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::lock_guard lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::DoubleDirSize() -> void {
  size_t original_num_buckets = num_buckets_;
  num_buckets_ <<= 1;
  global_depth_++;
  dir_.resize(num_buckets_);
  std::copy(dir_.begin(), dir_.begin() + original_num_buckets, dir_.begin() + original_num_buckets);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  for (const auto &[key, value] : bucket->GetItems()) {
    size_t redistribute_bucket_index = IndexOf(key);
    dir_[redistribute_bucket_index]->Insert(key, value);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard lock(latch_);

  auto bucket_index = IndexOf(key);
  auto bucket_ptr = dir_[bucket_index];

  auto flag = bucket_ptr->Find(key, value);
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard lock(latch_);

  auto bucket_index = IndexOf(key);
  auto bucket_ptr = dir_[bucket_index];

  auto flag = bucket_ptr->Remove(key);
  return flag;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard lock(latch_);

  auto bucket_index = IndexOf(key);
  auto bucket = dir_[bucket_index];

  while (!bucket->Insert(key, value)) {
    if (bucket->GetDepth() == global_depth_) {
      DoubleDirSize();
    }

    auto new_bucket1 = std::make_shared<Bucket>(bucket_size_, global_depth_);
    auto new_bucket2 = std::make_shared<Bucket>(bucket_size_, global_depth_);

    const unsigned shift_bits = global_depth_ - 1;
    for (int i = 0; i < num_buckets_; i++) {
      if (dir_[i] == bucket) {
        dir_[i] = i >> shift_bits == 0 ? new_bucket1 : new_bucket2;
      }
    }

    RedistributeBucket(bucket);

    bucket_index = IndexOf(key);
    bucket = dir_[bucket_index];
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(const size_t size, const int depth) : list_(), size_(size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto itr = std::find_if(list_.begin(), list_.end(), [&key](const auto &pair) -> bool { return pair.first == key; });

  if (itr == list_.end()) {
    return false;
  }

  value = itr->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto itr = std::find_if(list_.begin(), list_.end(), [&key](const auto &pair) -> bool { return pair.first == key; });

  if (itr == list_.end()) {
    return false;
  }

  list_.erase(itr);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // For performance, not use IsFull() member function.
  if (list_.size() == size_) {
    return false;
  }

  auto itr = std::find_if(list_.begin(), list_.end(), [&key](const auto &pair) -> bool { return pair.first == key; });

  if (itr != list_.end()) {
    itr->second = value;
  } else {
    list_.emplace_back(key, value);
  }

  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
//  test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;
}  // namespace bustub
