// DO NOT CHANGE THIS FILE, this file will not be included in the autograder.

#include <exception>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::UpdateUndoLink(const RID rid, std::optional<UndoLink> prev_link,
                                        std::function<bool(std::optional<UndoLink>)> &&check) -> bool {
  std::function wrapper_func = [check](std::optional<VersionUndoLink> link) -> bool {
    if (link.has_value()) {
      return check(link->prev_);
    }
    return check(std::nullopt);
  };
  return UpdateVersionLink(rid, prev_link.has_value() ? std::make_optional(VersionUndoLink{*prev_link}) : std::nullopt,
                           check != nullptr ? wrapper_func : nullptr);
}

auto TransactionManager::UpdateVersionLink(const RID rid, std::optional<VersionUndoLink> prev_version,
                                           std::function<bool(std::optional<VersionUndoLink>)> &&check) -> bool {
  std::unique_lock lck(version_info_mutex_);
  std::shared_ptr<PageVersionInfo> pg_ver_info = nullptr;
  if (const auto itr = version_info_.find(rid.GetPageId()); itr == version_info_.end()) {
    pg_ver_info = std::make_shared<PageVersionInfo>();
    version_info_[rid.GetPageId()] = pg_ver_info;
  } else {
    pg_ver_info = itr->second;
  }
  std::unique_lock lck2(pg_ver_info->mutex_);
  lck.unlock();
  if (const auto itr2 = pg_ver_info->prev_version_.find(rid.GetSlotNum()); itr2 == pg_ver_info->prev_version_.end()) {
    if (check != nullptr && !check(std::nullopt)) {
      return false;
    }
  } else {
    if (check != nullptr && !check(itr2->second)) {
      return false;
    }
  }
  if (prev_version.has_value()) {
    pg_ver_info->prev_version_[rid.GetSlotNum()] = *prev_version;
  } else {
    pg_ver_info->prev_version_.erase(rid.GetSlotNum());
  }
  return true;
}

auto TransactionManager::GetVersionLink(const RID rid) -> std::optional<VersionUndoLink> {
  std::shared_lock lck(version_info_mutex_);
  const auto itr = version_info_.find(rid.GetPageId());
  if (itr == version_info_.end()) {
    return std::nullopt;
  }
  const std::shared_ptr<PageVersionInfo> pg_ver_info = itr->second;
  std::unique_lock lck2(pg_ver_info->mutex_);
  lck.unlock();
  const auto itr2 = pg_ver_info->prev_version_.find(rid.GetSlotNum());
  if (itr2 == pg_ver_info->prev_version_.end()) {
    return std::nullopt;
  }
  return std::make_optional(itr2->second);
}

auto TransactionManager::GetUndoLink(const RID rid) -> std::optional<UndoLink> {
  if (auto version_link = GetVersionLink(rid); version_link.has_value()) {
    return version_link->prev_;
  }
  return std::nullopt;
}

auto TransactionManager::GetUndoLogOptional(UndoLink link) -> std::optional<UndoLog> {
  std::shared_lock lck(txn_map_mutex_);
  const auto itr = txn_map_.find(link.prev_txn_);
  if (itr == txn_map_.end()) {
    return std::nullopt;
  }
  const auto &txn = itr->second;
  lck.unlock();
  return txn->GetUndoLog(link.prev_log_idx_);
}

auto TransactionManager::GetUndoLog(const UndoLink link) -> UndoLog {
  if (auto undo_log = GetUndoLogOptional(link); undo_log.has_value()) {
    return *undo_log;
  }
  throw Exception("undo log not exist");
}

void Transaction::SetTainted() {
  auto state = state_.load();
  if (state == TransactionState::RUNNING) {
    state_.store(TransactionState::TAINTED);
    return;
  }
  fmt::println(stderr, "transaction not in running state: {}", state);
  std::terminate();
}

}  // namespace bustub
