#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) {}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = std::exchange(that.bpm_, nullptr);
  page_ = std::exchange(that.page_, nullptr);
  is_dirty_ = std::exchange(that.is_dirty_, false);
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

BasicPageGuard::~BasicPageGuard() {
  if (page_) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : BasicPageGuard(bpm, page) { page_->RLatch(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : BasicPageGuard(std::move(that)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  BasicPageGuard::operator=(std::move(that));
  return *this;
}

ReadPageGuard::~ReadPageGuard() {
  if (page_) {
    page_->RUnlatch();
  }
}

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : BasicPageGuard(bpm, page) { page_->WLatch(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : BasicPageGuard(std::move(that)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  BasicPageGuard::operator=(std::move(that));
  return *this;
}

WritePageGuard::~WritePageGuard() {
  if (page_) {
    page_->WUnlatch();
  }
}

}  // namespace bustub
