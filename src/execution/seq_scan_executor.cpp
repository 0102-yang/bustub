//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  LOG_TRACE("Sequential Scan executor Init");
  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_TRACE("Sequential Scan executor Next");
  while (!table_iterator_->IsEnd()) {
    // Get next tuple.
    const auto [next_tuple_meta, next_tuple] = table_iterator_->GetTuple();
    const auto next_rid = table_iterator_->GetRID();
    ++*table_iterator_;

    if (next_tuple_meta.is_deleted_) {
      LOG_DEBUG("Failed to get tuple %s due to tuple is deleted", next_tuple.ToString(&GetOutputSchema()).c_str());
    } else if (plan_->filter_predicate_ &&
               !plan_->filter_predicate_->Evaluate(&next_tuple, GetOutputSchema()).GetAs<bool>()) {
      LOG_DEBUG("Failed to get tuple %s due to dissatify contidions", next_tuple.ToString(&GetOutputSchema()).c_str());
    } else {
      *tuple = next_tuple;
      *rid = next_rid;
      LOG_DEBUG("Succeed in getting tuple %s, RID %s in seqscan.", tuple->ToString(&GetOutputSchema()).c_str(),
                rid->ToString().c_str());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
