//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           const uint32_t header_max_depth,
                                                           const uint32_t directory_max_depth,
                                                           const uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  const auto header_page = bpm->NewPageGuarded(&header_page_id_).AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // Get bucket.
  // Get header page.
  const auto header = bpm_->FetchPageRead(header_page_id_).As<ExtendibleHTableHeaderPage>();

  // Get directory page.
  const auto key_hash = Hash(key);
  const auto directory_idx = header->HashToDirectoryIndex(key_hash);
  const page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory = bpm_->FetchPageRead(directory_page_id).As<ExtendibleHTableDirectoryPage>();

  // Get bucket page.
  const auto bucket_idx = directory->HashToBucketIndex(key_hash);
  const page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // Get value.
  auto bucket = bpm_->FetchPageRead(bucket_page_id).As<ExtendibleHTableBucketPage<K, V, KC>>();
  if (V value; bucket->Lookup(key, value, cmp_)) {
    LOG_DEBUG("Find %u in the bucket %u of the directory %u", key_hash, bucket_page_id, directory_page_id);
    result->emplace_back(value);
    return true;
  }
  LOG_DEBUG("No corresponding key %u was found in the bucket %u of the directory %u", key_hash, bucket_page_id,
            directory_page_id);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  const auto header = bpm_->FetchPageWrite(header_page_id_).AsMut<ExtendibleHTableHeaderPage>();
  const uint32_t key_hash = Hash(key);
  return InsertToNewDirectory(header, key_hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, const uint32_t hash,
                                                             const K &key, const V &value) -> bool {
  const auto directory_idx = header->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    // Create new directory.
    const auto new_directory = bpm_->NewPageGuarded(&directory_page_id).AsMut<ExtendibleHTableDirectoryPage>();
    new_directory->Init(directory_max_depth_);
    header->SetDirectoryPageId(directory_idx, directory_page_id);
  }
  const auto directory = bpm_->FetchPageWrite(directory_page_id).AsMut<ExtendibleHTableDirectoryPage>();
  LOG_DEBUG("Insert key %u into directory %u", hash, directory_page_id);
  return InsertToNewBucket(directory, hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, const uint32_t hash,
                                                          const K &key, const V &value) -> bool {
  const auto bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  // Create new bucket.
  if (bucket_page_id == INVALID_PAGE_ID) {
    const auto new_bucket = bpm_->NewPageGuarded(&bucket_page_id).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    directory->SetLocalDepth(bucket_idx, 0U);
  }

  auto bucket = bpm_->FetchPageWrite(bucket_page_id).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  /** Split bucket. */
  bool is_split = false;
  while (bucket->IsFull()) {
    uint8_t local_depth = static_cast<uint8_t>(directory->GetLocalDepth(bucket_idx));
    if (local_depth == directory->GetGlobalDepth()) {
      if (directory->Size() == directory->MaxSize()) {
        // There is not enough space for insert.
        return false;
      }
      // Expand directory.
      LOG_DEBUG("Directory increment global size at insert %u", hash);
      directory->IncrGlobalDepth();
    }

    // Update directory mapping for new local depth.
    ++local_depth;
    const auto old_depth_mask = directory->GetLocalDepthMask(bucket_idx);
    UpdateDirectoryLocalDepthMapping(directory, bucket_idx, local_depth, old_depth_mask);

    // Create new bucket.
    page_id_t new_bucket_page_id;
    auto new_bucket = bpm_->NewPageGuarded(&new_bucket_page_id).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);

    // Update mapping.
    const auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    const auto new_depth_mask = old_depth_mask + (1 << (local_depth - 1));
    UpdateDirectoryPageIdMapping(directory, new_bucket_idx, new_bucket_page_id, new_depth_mask);
    UpdateDirectoryLocalDepthMapping(directory, new_bucket_idx, local_depth, new_depth_mask);

    // Migrate entries.
    MigrateEntries(bucket, new_bucket, new_bucket_idx, new_depth_mask);
    is_split = true;
  }
  if (is_split) {
    return InsertToNewBucket(directory, hash, key, value);
  }
  auto insert_success = bucket->Insert(key, value, cmp_);
  if (insert_success) {
    LOG_DEBUG("Insert key %u into bucket %u", hash, bucket_page_id);
  }
  return insert_success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  // Get header page.
  const auto header = bpm_->FetchPageRead(header_page_id_).As<ExtendibleHTableHeaderPage>();

  // Get directory page.
  const auto key_hash = Hash(key);
  const auto directory_idx = header->HashToDirectoryIndex(key_hash);
  const page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory = bpm_->FetchPageWrite(directory_page_id).AsMut<ExtendibleHTableDirectoryPage>();

  // Get bucket page.
  const auto bucket_idx = directory->HashToBucketIndex(key_hash);
  const page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // Remove key.
  auto bucket = bpm_->FetchPageWrite(bucket_page_id).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto is_removed = bucket->Remove(key, cmp_);
  if (is_removed) {
    LOG_DEBUG("Remove %u at bucket page %d", key_hash, bucket_page_id);
  }
  if (is_removed && bucket->IsEmpty()) {
    // Delete bucket page.
    bpm_->DeletePage(bucket_page_id);
    // Update directory.
    const auto merged_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
    if (merged_bucket_idx == bucket_idx) {
      // The last bucket in this directory is about to be deleted.
      // Directly delete directory page.
      LOG_DEBUG("Delete page %d at directory %d", bucket_page_id, directory_page_id);
      auto writeable_header = bpm_->FetchPageWrite(header_page_id_).AsMut<ExtendibleHTableHeaderPage>();
      writeable_header->SetDirectoryPageId(directory_idx, INVALID_PAGE_ID);
      bpm_->DeletePage(directory_page_id);
    } else {
      const auto merged_bucket_page_id = directory->GetBucketPageId(merged_bucket_idx);
      const uint8_t merged_bucket_local_depth = static_cast<uint8_t>(directory->GetLocalDepth(bucket_idx) - 1);
      const uint32_t new_local_depth_mask = (directory->GetLocalDepthMask(bucket_idx)) >> 1;
      UpdateDirectoryPageIdMapping(directory, merged_bucket_idx, merged_bucket_page_id, new_local_depth_mask);
      UpdateDirectoryLocalDepthMapping(directory, merged_bucket_idx, merged_bucket_local_depth, new_local_depth_mask);

      // If directory can be shrunk, shrink it.
      if (directory->CanShrink()) {
        LOG_DEBUG("Decrement directory %d global depth", directory_page_id);
        directory->DecrGlobalDepth();
      }
    }
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       const uint32_t new_bucket_idx, const uint32_t local_depth_mask) {
  const uint32_t lower_bits = new_bucket_idx & local_depth_mask;
  for (uint32_t i = old_bucket->Size() - 1U; static_cast<int>(i) >= 0; --i) {
    if (const auto &[k, v] = old_bucket->EntryAt(i); (Hash(k) & local_depth_mask) == lower_bits) {
      // Need insert to new bucket.
      new_bucket->Insert(k, v, cmp_);
      old_bucket->RemoveAt(i);
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryPageIdMapping(ExtendibleHTableDirectoryPage *directory,
                                                                     const uint32_t bucket_idx,
                                                                     const page_id_t new_bucket_page_id,
                                                                     const uint32_t local_depth_mask) {
  const uint32_t lower_bits = bucket_idx & local_depth_mask;
  for (uint32_t idx = 0U; idx < directory->Size(); ++idx) {
    if ((idx & local_depth_mask) == lower_bits) {
      directory->SetBucketPageId(idx, new_bucket_page_id);
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryLocalDepthMapping(ExtendibleHTableDirectoryPage *directory,
                                                                         const uint32_t bucket_idx,
                                                                         const uint8_t new_local_depth,
                                                                         const uint32_t local_depth_mask) {
  const uint32_t lower_bits = bucket_idx & local_depth_mask;
  for (uint32_t idx = 0U; idx < directory->Size(); ++idx) {
    if ((idx & local_depth_mask) == lower_bits) {
      directory->SetLocalDepth(idx, new_local_depth);
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
