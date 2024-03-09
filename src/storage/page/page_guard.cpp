#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = std::exchange(that.bpm_, nullptr);
  page_ = std::exchange(that.page_, nullptr);
  is_dirty_ = std::exchange(that.is_dirty_, false);
}

void BasicPageGuard::Drop() {
  if (bpm_ && page_) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    if (page_ && PageId() != that.PageId()) {
      bpm_->UnpinPage(PageId(), is_dirty_);
    }
    bpm_ = std::exchange(that.bpm_, nullptr);
    page_ = std::exchange(that.page_, nullptr);
    is_dirty_ = std::exchange(that.is_dirty_, false);
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ && guard_.page_) {
    guard_.page_->RUnlatch();
    LOG_TRACE("Release read lock of page %d successfully.", PageId());
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ && guard_.page_) {
    guard_.page_->WUnlatch();
    LOG_TRACE("Release write lock of page %d successfully.", PageId());
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub