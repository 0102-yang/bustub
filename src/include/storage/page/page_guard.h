#pragma once

#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;

class BasicPageGuard {
 public:
  BasicPageGuard(BufferPoolManager *bpm, Page *page);
  BasicPageGuard(const BasicPageGuard &) = delete;
  auto operator=(const BasicPageGuard &) -> BasicPageGuard & = delete;

  BasicPageGuard(BasicPageGuard &&that) noexcept;

  auto operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard &;

  [[nodiscard]] auto PageId() const -> page_id_t { return page_->GetPageId(); }

  [[nodiscard]] auto GetData() const -> const char * { return page_->GetData(); }

  virtual ~BasicPageGuard() = 0;

  template <class T>
  auto As() -> const T * {
    const char *data = page_->GetData();
    return reinterpret_cast<const T *>(data);
  }

 protected:
  BufferPoolManager *bpm_;
  Page *page_;
  bool is_dirty_{false};
};

class ReadPageGuard : public BasicPageGuard {
 public:
  ReadPageGuard(BufferPoolManager *bpm, Page *page);
  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  /** TODO(P2): Add implementation
   *
   * @brief Move constructor for ReadPageGuard
   *
   * Very similar to BasicPageGuard. You want to create
   * a ReadPageGuard using another ReadPageGuard. Think
   * about if there's any way you can make this easier for yourself...
   */
  ReadPageGuard(ReadPageGuard &&that) noexcept;

  /** TODO(P2): Add implementation
   *
   * @brief Move assignment for ReadPageGuard
   *
   * Very similar to BasicPageGuard. Given another ReadPageGuard,
   * replace the contents of this one with that one.
   */
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;

  /** TODO(P2): Add implementation
   *
   * @brief Destructor for ReadPageGuard
   *
   * Just like with BasicPageGuard, this should behave
   * as if you were dropping the guard.
   */
  ~ReadPageGuard() override;
};

class WritePageGuard : public BasicPageGuard {
 public:
  WritePageGuard(BufferPoolManager *bpm, Page *page);
  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;

  /** TODO(P2): Add implementation
   *
   * @brief Move constructor for WritePageGuard
   *
   * Very similar to BasicPageGuard. You want to create
   * a WritePageGuard using another WritePageGuard. Think
   * about if there's any way you can make this easier for yourself...
   */
  WritePageGuard(WritePageGuard &&that) noexcept;

  /** TODO(P2): Add implementation
   *
   * @brief Move assignment for WritePageGuard
   *
   * Very similar to BasicPageGuard. Given another WritePageGuard,
   * replace the contents of this one with that one.
   */
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;

  /** TODO(P2): Add implementation
   *
   * @brief Destructor for WritePageGuard
   *
   * Just like with BasicPageGuard, this should behave
   * as if you were dropping the guard.
   */
  ~WritePageGuard() override;

  template <class T>
  auto AsMut() -> T * {
    is_dirty_ = true;
    char *data = page_->GetData();
    return reinterpret_cast<T *>(data);
  }
};

}  // namespace bustub
