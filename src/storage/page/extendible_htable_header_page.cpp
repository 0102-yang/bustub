//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(const uint32_t max_depth) {
  max_depth_ = std::min(max_depth, static_cast<uint32_t>(HTABLE_HEADER_MAX_DEPTH));
  std::fill_n(std::begin(directory_page_ids_), MaxSize(), INVALID_PAGE_ID);
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(const uint32_t hash) const -> uint32_t {
  if (MaxSize() == 1) {
    return 0U;
  }
  return (hash >> (32 - max_depth_)) & ((1 << max_depth_) - 1U);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(const uint32_t directory_idx) const -> uint32_t {
  BUSTUB_ASSERT(directory_idx < MaxSize(), "Out of range");
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(const uint32_t directory_idx, const page_id_t directory_page_id) {
  BUSTUB_ASSERT(directory_idx < MaxSize(), "Out of range");
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

}  // namespace bustub
