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
  const auto catalog = exec_ctx_->GetCatalog();
  const auto table_info = catalog->GetTable(plan_->table_oid_);
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_TRACE("Sequential Scan executor Next");
  while (!table_iterator_->IsEnd()) {
    // Get next tuple.
    const auto [next_tuple_meta, next_tuple] = table_iterator_->GetTuple();

    if (next_tuple_meta.is_deleted_ ||
        (plan_->filter_predicate_ &&
         !plan_->filter_predicate_->Evaluate(&next_tuple, GetOutputSchema()).GetAs<bool>())) {
      ++*table_iterator_;
      LOG_DEBUG("Failed to get tuple %s due to dissatify contidions", next_tuple.ToString(&GetOutputSchema()).c_str());
      continue;
    }

    *tuple = next_tuple;
    *rid = table_iterator_->GetRID();
    LOG_DEBUG("Succeed in getting tuple %s, RID %s in seqscan.", tuple->ToString(&GetOutputSchema()).c_str(),
              rid->ToString().c_str());
    ++*table_iterator_;
    return true;
  }

  LOG_TRACE("No more tuple");
  return false;
}

}  // namespace bustub
