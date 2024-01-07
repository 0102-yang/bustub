//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(const uint32_t max_size) {
  size_ = 0U;
  max_size_ = std::min(max_size, static_cast<uint32_t>(HTableBucketArraySize(sizeof(std::pair<K, V>))));
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  auto it = std::find_if(std::begin(array_), std::begin(array_) + size_,
                         [&cmp, &key](const auto &pair) { return cmp(key, pair.first) == 0; });
  if (it != std::begin(array_) + size_) {
    // Found key.
    value = it->second;
    return true;
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  auto it = std::find_if(std::begin(array_), std::begin(array_) + size_,
                         [&cmp, &key](const auto &pair) { return cmp(key, pair.first) == 0; });
  // If key exist, insert fail.
  // If key does not exist, add it to the end of the array.
  if (it != std::begin(array_) + size_) {
    return false;
  }
  if (size_ == max_size_) {
    return false;
  }
  array_[size_++] = {key, value};
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  auto it = std::find_if(std::begin(array_), std::begin(array_) + size_,
                         [&key, &cmp](const auto &pair) { return cmp(key, pair.first) == 0; });
  if (it == std::begin(array_) + size_) {
    // Key not found.
    return false;
  }

  // Remove.
  std::copy(it + 1, std::begin(array_) + size_, it);
  --size_;
  return true;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(const uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  // Remove.
  auto it = std::begin(array_) + bucket_idx;
  std::copy(it + 1, std::begin(array_) + size_, it);
  --size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(const uint32_t bucket_idx) const -> K {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(const uint32_t bucket_idx) const -> V {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(const uint32_t bucket_idx) const -> const std::pair<K, V> & {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0U;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
