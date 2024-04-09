//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  LOG_DEBUG("Initialize index scan executor with plan:\n%s", plan_->ToString().c_str());
}

void IndexScanExecutor::Init() {
  const auto catalog = exec_ctx_->GetCatalog();
  const auto index_info = catalog->GetIndex(plan_->GetIndexOid());
  const auto hash_table_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  const Schema key_schema({Column("key", INTEGER)});
  const auto value = plan_->pred_key_->Evaluate(nullptr, GetOutputSchema());
  hash_table_index->ScanKey({{value}, &key_schema}, &rids_, nullptr);
  LOG_TRACE("Index scanned %lu rid(s)", rids_.size());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);

  while (scan_index_ < rids_.size()) {
    const auto next_rid = rids_[scan_index_++];
    const auto [next_tuple_meta, next_tuple] = table_info->table_->GetTuple(next_rid);

    if (next_tuple_meta.is_deleted_) {
      continue;
    }

    *tuple = next_tuple;
    *rid = next_rid;
    LOG_TRACE("Get tuple - %s, rid - %s from index scan", tuple->ToString(&GetOutputSchema()).c_str(),
              rid->ToString().c_str());
    return true;
  }

  return false;
}

}  // namespace bustub
