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
    : AbstractExecutor(exec_ctx), plan_(plan) {
  LOG_DEBUG("Initialize sequential scan executor.\n%s", plan_->ToString().c_str());
}

void SeqScanExecutor::Init() {
  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto output_schema = GetOutputSchema();
  const auto predicate = plan_->filter_predicate_;
  while (!table_iterator_->IsEnd()) {
    // Get next tuple.
    const auto [next_tuple_meta, next_tuple] = table_iterator_->GetTuple();
    const auto next_rid = table_iterator_->GetRID();
    ++*table_iterator_;

    if (next_tuple_meta.is_deleted_) {
      LOG_TRACE("Failed to get tuple %s due to tuple is deleted", next_tuple.ToString(&output_schema).c_str());
      continue;
    }
    if (predicate && !predicate->Evaluate(&next_tuple, output_schema).GetAs<bool>()) {
      LOG_TRACE("Failed to get tuple %s due to dissatify contidions", next_tuple.ToString(&output_schema).c_str());
      continue;
    }

    *tuple = next_tuple;
    *rid = next_rid;
    LOG_TRACE("Succeed in getting tuple %s, RID %s in sequential scan.", tuple->ToString(&output_schema).c_str(),
              rid->ToString().c_str());
    return true;
  }

  return false;
}

}  // namespace bustub
