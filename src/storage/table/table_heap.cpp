//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_heap.cpp
//
// Identification: src/storage/table/table_heap.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <mutex>  // NOLINT
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "fmt/format.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"
#include "storage/table/table_heap.h"

namespace bustub {

TableHeap::TableHeap(BufferPoolManager *bpm) : bpm_(bpm) {
  // Initialize the first table page.
  auto guard = bpm->NewPageGuarded(&first_page_id_);
  last_page_id_ = first_page_id_;
  const auto first_page = guard.AsMut<TablePage>();
  BUSTUB_ASSERT(first_page != nullptr,
                "Couldn't create a page for the table heap. Have you completed the buffer pool manager project?");
  first_page->Init();
}

TableHeap::TableHeap(bool create_table_heap) : bpm_(nullptr) {}

auto TableHeap::InsertTuple(const TupleMeta &meta, const Tuple &tuple, LockManager *lock_mgr, Transaction *txn,
                            table_oid_t oid) -> std::optional<RID> {
  std::unique_lock guard(latch_);
  auto page_guard = bpm_->FetchPageWrite(last_page_id_);
  while (true) {
    const auto page = page_guard.AsMut<TablePage>();
    if (page->GetNextTupleOffset(meta, tuple) != std::nullopt) {
      break;
    }

    // if there's no tuple in the page, and we can't insert the tuple, then this tuple is too large.
    BUSTUB_ENSURE(page->GetNumTuples() != 0, "tuple is too large, cannot insert");

    page_id_t next_page_id = INVALID_PAGE_ID;
    const auto npg = bpm_->NewPage(&next_page_id);
    BUSTUB_ENSURE(next_page_id != INVALID_PAGE_ID, "cannot allocate page");

    page->SetNextPageId(next_page_id);

    const auto next_page = reinterpret_cast<TablePage *>(npg->GetData());
    next_page->Init();

    // acquire latch here as TSAN complains. Given we only have one insertion thread, this is fine.
    npg->WLatch();
    auto next_page_guard = WritePageGuard{bpm_, npg};

    last_page_id_ = next_page_id;
    page_guard = std::move(next_page_guard);
  }
  auto last_page_id = last_page_id_;

  const auto page = page_guard.AsMut<TablePage>();
  auto slot_id = *page->InsertTuple(meta, tuple);

  // only allow one insertion at a time, otherwise it will deadlock.
  guard.unlock();

#ifndef DISABLE_LOCK_MANAGER
  if (lock_mgr != nullptr) {
    BUSTUB_ENSURE(lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, oid, RID{last_page_id, slot_id}),
                  "failed to lock when inserting new tuple");
  }
#endif

  return RID(last_page_id, slot_id);
}

void TableHeap::UpdateTupleMeta(const TupleMeta &meta, const RID rid) const {
  auto page_guard = bpm_->FetchPageWrite(rid.GetPageId());
  auto page = page_guard.AsMut<TablePage>();
  page->UpdateTupleMeta(meta, rid);
}

auto TableHeap::GetTuple(const RID rid) const -> std::pair<TupleMeta, Tuple> {
  auto page_guard = bpm_->FetchPageRead(rid.GetPageId());
  const auto page = page_guard.As<TablePage>();
  auto [meta, tuple] = page->GetTuple(rid);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto TableHeap::GetTupleMeta(const RID rid) const -> TupleMeta {
  auto page_guard = bpm_->FetchPageRead(rid.GetPageId());
  const auto page = page_guard.As<TablePage>();
  return page->GetTupleMeta(rid);
}

auto TableHeap::MakeIterator() -> TableIterator {
  std::unique_lock guard(latch_);
  auto last_page_id = last_page_id_;
  guard.unlock();

  auto page_guard = bpm_->FetchPageRead(last_page_id);
  const auto page = page_guard.As<TablePage>();
  auto num_tuples = page->GetNumTuples();
  return {this, {first_page_id_, 0}, {last_page_id, num_tuples}};
}

auto TableHeap::MakeEagerIterator() -> TableIterator { return {this, {first_page_id_, 0}, {INVALID_PAGE_ID, 0}}; }

auto TableHeap::UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, const RID rid,
                                   std::function<bool(const TupleMeta &m, const Tuple &table, RID r)> &&check) const
    -> bool {
  auto page_guard = bpm_->FetchPageWrite(rid.GetPageId());
  const auto page = page_guard.AsMut<TablePage>();
  if (auto [old_meta, old_tup] = page->GetTuple(rid); check == nullptr || check(old_meta, old_tup, rid)) {
    page->UpdateTupleInPlaceUnsafe(meta, tuple, rid);
    return true;
  }
  return false;
}

auto TableHeap::AcquireTablePageReadLock(const RID rid) const -> ReadPageGuard {
  return bpm_->FetchPageRead(rid.GetPageId());
}

auto TableHeap::AcquireTablePageWriteLock(const RID rid) const -> WritePageGuard {
  return bpm_->FetchPageWrite(rid.GetPageId());
}

void TableHeap::UpdateTupleInPlaceWithLockAcquired(const TupleMeta &meta, const Tuple &tuple, const RID rid,
                                                   TablePage *page) {
  page->UpdateTupleInPlaceUnsafe(meta, tuple, rid);
}

auto TableHeap::GetTupleWithLockAcquired(const RID rid, const TablePage *page) -> std::pair<TupleMeta, Tuple> {
  auto [meta, tuple] = page->GetTuple(rid);
  tuple.rid_ = rid;
  return std::make_pair(meta, std::move(tuple));
}

auto TableHeap::GetTupleMetaWithLockAcquired(const RID rid, const TablePage *page) -> TupleMeta {
  return page->GetTupleMeta(rid);
}

}  // namespace bustub
