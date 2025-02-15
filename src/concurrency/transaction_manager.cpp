//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::lock_guard l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();

  txn_map_.emplace(txn_id, std::move(txn));
  txn_ref->read_ts_ = running_txns_.GetLatestCommitTimestamp();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock commit_lck(commit_mutex_);

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  const timestamp_t commit_ts = ++last_commit_ts_;

  // Iterate through all tuples changed by this transaction and update their commit timestamp
  // to the commit timestamp of this transaction.
  for (const auto &[table_oid, write_rids] : txn->GetWriteSets()) {
    const auto *table_heap = catalog_->GetTable(table_oid)->table_.get();
    for (const auto &rid : write_rids) {
      // Set the timestamp of the base tuples to the commit timestamp.
      auto meta = table_heap->GetTupleMeta(rid);
      meta.ts_ = commit_ts;
      table_heap->UpdateTupleMeta(meta, rid);
    }
  }

  // Update transaction.
  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // Get the watermark.
  const auto watermark = running_txns_.GetWatermark();

  // Mark visible undo logs.
  std::unordered_map<txn_id_t, uint> txn_visible_undo_logs_counts;
  std::shared_lock lck(txn_map_mutex_);
  for (const auto &[txn_id_, txn_] : txn_map_) {
    txn_visible_undo_logs_counts[txn_id_] = static_cast<uint>(txn_->GetUndoLogNum());
  }

  for (const auto &[page_id, page_version_info] : version_info_) {
    for (const auto &[slot_num, version_link] : page_version_info->prev_version_) {
      auto link = version_link.prev_;
      bool is_first_undo_log = true;
      const auto tuple_ts =
          catalog_->GetTable(page_id)->table_->GetTupleMeta({page_id, static_cast<uint32_t>(slot_num)}).ts_;
      while (link.IsValid()) {
        if (IsDanglingUndoLink(link, this)) {
          break;
        }

        const auto &[is_deleted_, modified_fields_, tuple_, ts_, prev_version_] = GetUndoLog(link);
        if (txn_visible_undo_logs_counts.find(link.prev_txn_) != txn_visible_undo_logs_counts.end()) {
          if (ts_ < watermark) {
            if (!is_first_undo_log || tuple_ts <= watermark) {
              txn_visible_undo_logs_counts[link.prev_txn_]--;
            }
          }
        }

        if (is_first_undo_log) {
          is_first_undo_log = false;
        }
        link = prev_version_;
      }
    }
  }

  // Remove unused transactions.
  for (const auto &[txn_id_, undo_logs_counts] : txn_visible_undo_logs_counts) {
    const auto txn = txn_map_[txn_id_];
    if (const auto txn_state = txn->GetTransactionState();
        undo_logs_counts == 0 && (txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED)) {
      txn_map_.erase(txn_id_);
    }
  }
}

}  // namespace bustub
