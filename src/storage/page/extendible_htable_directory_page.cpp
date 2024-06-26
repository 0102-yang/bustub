//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(const uint32_t max_depth) {
  max_depth_ = std::min(max_depth, static_cast<uint32_t>(HTABLE_DIRECTORY_MAX_DEPTH));
  const auto size = MaxSize();
  std::fill_n(std::begin(bucket_page_ids_), size, INVALID_PAGE_ID);
  std::fill_n(std::begin(local_depths_), size, 0U);
  global_depth_ = 0;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(const uint32_t hash) const -> uint32_t { return hash % Size(); }

auto ExtendibleHTableDirectoryPage::GetBucketPageId(const uint32_t bucket_idx) const -> page_id_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(const uint32_t bucket_idx, const page_id_t bucket_page_id) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(const uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  if (Size() == 1U) {
    // Only 1 bucket.
    return bucket_idx;
  }
  const uint32_t half_size = 1 << (global_depth_ - 1);
  return bucket_idx < half_size ? bucket_idx + half_size : bucket_idx - half_size;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(const uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  return (1 << local_depths_[bucket_idx]) - 1U;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ >= max_depth_) {
    return;
  }
  const auto original_size = Size();
  std::copy_n(std::begin(local_depths_), original_size, std::begin(local_depths_) + original_size);
  std::copy_n(std::begin(bucket_page_ids_), original_size, std::begin(bucket_page_ids_) + original_size);
  ++global_depth_;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    return;
  }
  const uint32_t half_size = Size() / 2;
  std::fill_n(std::begin(bucket_page_ids_) + half_size, half_size, INVALID_PAGE_ID);
  std::fill_n(std::begin(local_depths_) + half_size, half_size, 0);
  --global_depth_;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  return std::all_of(std::begin(local_depths_), std::begin(local_depths_) + Size(),
                     [&](const uint32_t depth) { return depth < global_depth_; });
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(const uint32_t bucket_idx) const -> uint32_t {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(const uint32_t bucket_idx, const uint8_t local_depth) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(const uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  ++local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(const uint32_t bucket_idx) {
  BUSTUB_ASSERT(bucket_idx < Size(), "Out of range");
  --local_depths_[bucket_idx];
}

}  // namespace bustub
