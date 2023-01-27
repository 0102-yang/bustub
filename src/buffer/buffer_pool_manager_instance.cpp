//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t replaceable_frame_id;
  if (!GetFreeFrameId(replaceable_frame_id)) {
    return nullptr;
  }

  Page *page = &pages_[replaceable_frame_id];
  const page_id_t new_page_id = AllocatePage();
  ResetPageMetaDataToDefault(page, new_page_id);

  page_table_->Insert(new_page_id, replaceable_frame_id);
  replacer_->RecordAccess(replaceable_frame_id);
  replacer_->SetEvictable(replaceable_frame_id, false);

  *page_id = new_page_id;
  return &pages_[replaceable_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(const page_id_t page_id) -> Page * {
  frame_id_t replaceable_frame_id;
  Page *page;

  if (page_table_->Find(page_id, replaceable_frame_id)) {
    page = &pages_[replaceable_frame_id];
    page->pin_count_++;
    return &pages_[replaceable_frame_id];
  }

  if (!GetFreeFrameId(replaceable_frame_id)) {
    return nullptr;
  }

  page = &pages_[replaceable_frame_id];
  ResetPageMetaDataToDefault(page, page_id);
  disk_manager_->ReadPage(page_id, page->data_);

  page_table_->Insert(page_id, replaceable_frame_id);
  replacer_->RecordAccess(replaceable_frame_id);
  replacer_->SetEvictable(replaceable_frame_id, false);

  return &pages_[replaceable_frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(const page_id_t page_id, const bool is_dirty) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  if (page->GetPinCount() == 0) {
    return false;
  }

  page->is_dirty_ = is_dirty;
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(const page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(pages_[i].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(const page_id_t page_id) -> bool {
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *page = &pages_[frame_id];
  if (page->pin_count_ == 0) {
    return false;
  }

  ClearPage(page);
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetFreeFrameId(frame_id_t &frame_id) -> bool {
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    frame_id_t replaceable_frame_id;
    if (!replacer_->Evict(&replaceable_frame_id)) {
      return false;
    }

    Page *replaceable_page = &pages_[replaceable_frame_id];
    if (replaceable_page->IsDirty()) {
      disk_manager_->WritePage(replaceable_page->page_id_, replaceable_page->data_);
    }

    page_table_->Remove(replaceable_page->page_id_);
    replacer_->Remove(replaceable_frame_id);

    frame_id = replaceable_frame_id;
  }

  return true;
}

}  // namespace bustub
