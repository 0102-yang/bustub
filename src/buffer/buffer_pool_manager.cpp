//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard lock(latch_);

  /********************
   * Get free frame id.
   ********************/
  auto free_frame_id = GetFreeFrameId();
  if (!free_frame_id) {
    return nullptr;
  }

  /********************
   * Generate new page.
   ********************/
  // Get a new page.
  Page *new_page = &pages_[*free_frame_id];
  // Flush original page data to disk if necessary.
  if (new_page->IsDirty()) {
    auto finish_promise = DiskScheduler::CreatePromise();
    auto finish_flag = finish_promise.get_future();
    disk_scheduler_->Schedule({true, new_page->data_, new_page->page_id_, std::move(finish_promise)});
    if (!finish_flag.get()) {
      return nullptr;
    }
  }
  // Reset page data and metadata.
  ResetPageMetaDataToDefault(new_page, AllocatePage());
  new_page->ResetMemory();

  // Track new page and record page access.
  page_table_.emplace(new_page->page_id_, *free_frame_id);
  replacer_->RecordAccess(*free_frame_id);
  replacer_->SetEvictable(*free_frame_id, false);

  *page_id = new_page->page_id_;
  return new_page;
}

auto BufferPoolManager::FetchPage(const page_id_t page_id, const AccessType access_type) -> Page * {
  std::lock_guard lock(latch_);

  /****************************
   * Get Page from buffer pool.
   ****************************/
  if (page_table_.count(page_id) > 0) {
    // If page is already in buffer pool,
    // just return it.
    const frame_id_t fetch_page_frame_id = page_table_.at(page_id);
    Page *fetch_page = &pages_[fetch_page_frame_id];
    ++fetch_page->pin_count_;
    replacer_->RecordAccess(fetch_page_frame_id, access_type);
    replacer_->SetEvictable(fetch_page_frame_id, false);
    return fetch_page;
  }

  /*********************
   * Get Page from disk.
   *********************/
  // Get free frame id.
  auto free_frame_id = GetFreeFrameId();
  if (!free_frame_id) {
    // Return nullptr if page_id needs to be fetched from the disk
    // but all frames are currently in use and not evictable.
    return nullptr;
  }

  Page *fetch_page = &pages_[*free_frame_id];
  auto finish_promise = DiskScheduler::CreatePromise();
  auto finish_flag = finish_promise.get_future();
  disk_scheduler_->Schedule({false, fetch_page->data_, page_id, std::move(finish_promise)});
  ResetPageMetaDataToDefault(fetch_page, page_id);

  // Track fetch page and record page access.
  page_table_.emplace(fetch_page->page_id_, *free_frame_id);
  replacer_->Remove(*free_frame_id);
  replacer_->RecordAccess(*free_frame_id, access_type);
  replacer_->SetEvictable(*free_frame_id, false);

  if (!finish_flag.get()) {
    return nullptr;
  }

  return fetch_page;
}

auto BufferPoolManager::UnpinPage(const page_id_t page_id, const bool is_dirty,
                                  [[maybe_unused]] const AccessType access_type) -> bool {
  std::lock_guard lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }

  const frame_id_t unpin_page_frame_id = page_table_.at(page_id);
  Page *unpin_page = &pages_[unpin_page_frame_id];
  if (unpin_page->pin_count_ == 0) {
    return false;
  }

  // Configure unpin page.
  if (!unpin_page->is_dirty_ && is_dirty) {
    unpin_page->is_dirty_ = is_dirty;
  }
  if (--unpin_page->pin_count_ == 0) {
    replacer_->SetEvictable(unpin_page_frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(const page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::lock_guard lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }

  /*************
   * Flush page.
   *************/
  const frame_id_t page_frame = page_table_.at(page_id);
  Page *flush_page = &pages_[page_frame];

  auto finish_promise = DiskScheduler::CreatePromise();
  auto finish_flag = finish_promise.get_future();
  disk_scheduler_->Schedule({true, flush_page->data_, flush_page->page_id_, std::move(finish_promise)});
  flush_page->is_dirty_ = false;

  if (!finish_flag.get()) {
    return false;
  }

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPage(pages_[i].page_id_);
  }
}

auto BufferPoolManager::DeletePage(const page_id_t page_id) -> bool {
  std::lock_guard lock(latch_);

  if (page_table_.count(page_id) == 0) {
    // Page id not found.
    return true;
  }

  const frame_id_t page_frame = page_table_.at(page_id);
  Page *delete_page = &pages_[page_frame];
  if (delete_page->pin_count_ > 0) {
    return false;
  }

  /**************
   * Delete page.
   **************/
  delete_page->page_id_ = INVALID_PAGE_ID;
  delete_page->is_dirty_ = false;
  delete_page->ResetMemory();

  page_table_.erase(page_id);
  replacer_->Remove(page_frame);
  free_list_.push_back(page_frame);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageRead(const page_id_t page_id) -> ReadPageGuard {
  Page *fetch_page = FetchPage(page_id);
  LOG_DEBUG("Trying to acquire read lock of page %d.", page_id);
  fetch_page->RLatch();
  LOG_DEBUG("Acquire read lock of page %d successfully.", page_id);
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageWrite(const page_id_t page_id) -> WritePageGuard {
  Page *fetch_page = FetchPage(page_id);
  LOG_DEBUG("Trying to acquire write lock of page %d.", page_id);
  fetch_page->WLatch();
  LOG_DEBUG("Acquire write lock of page %d successfully.", page_id);
  return {this, fetch_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> WritePageGuard {
  Page *new_page = NewPage(page_id);
  LOG_DEBUG("Trying to acquire write lock of page %d.", *page_id);
  new_page->WLatch();
  LOG_DEBUG("Acquire write lock of page %d successfully.", *page_id);
  return {this, new_page};
}

auto BufferPoolManager::GetFreeFrameId() -> std::optional<frame_id_t> {
  if (free_list_.empty() && replacer_->Size() == 0) {
    return std::nullopt;
  }

  frame_id_t free_frame_id;
  if (free_list_.empty()) {
    // Get free frame id from lru-k replacer.
    // Write replaced page to disk if it is dirty.
    replacer_->Evict(&free_frame_id);
    Page *replaced_page = &pages_[free_frame_id];

    if (replaced_page->IsDirty()) {
      disk_scheduler_->Schedule({true, replaced_page->data_, replaced_page->page_id_, DiskScheduler::CreatePromise()});
    }

    page_table_.erase(replaced_page->page_id_);
  } else {
    // Get free frame id from free list.
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  }

  return std::make_optional(free_frame_id);
}

void BufferPoolManager::ResetPageMetaDataToDefault(Page *page, const page_id_t page_id) {
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
}

}  // namespace bustub
