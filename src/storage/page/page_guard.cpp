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
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    if (page_ != nullptr && PageId() != that.PageId()) {
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
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    LOG_TRACE("Released read lock of page %d", PageId());
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    LOG_TRACE("Released write lock of page %d", PageId());
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub