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
#include "common/logger.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iterator_(MakeIterator()) {
  LOG_DEBUG("Initialize sequential scan executor.\n%s", plan_->ToString().c_str());
}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &output_schema = GetOutputSchema();
  const auto &predicate = plan_->filter_predicate_;

  while (!table_iterator_.IsEnd()) {
    // Get next tuple.
    const auto [next_base_meta, next_base_tuple] = table_iterator_.GetTuple();
    const auto next_rid = table_iterator_.GetRID();
    ++table_iterator_;

    if (next_base_meta.is_deleted_) {
      LOG_TRACE("Failed to get tuple %s due to tuple is deleted", next_tuple.ToString(&output_schema).c_str());
      continue;
    }
    if (predicate && !predicate->Evaluate(&next_base_tuple, output_schema).GetAs<bool>()) {
      LOG_TRACE("Failed to get tuple %s due to dissatisfy conditions", next_tuple.ToString(&output_schema).c_str());
      continue;
    }

    *tuple = RetrieveTuple(next_base_tuple, next_base_meta, next_rid, output_schema);
    *rid = next_rid;
    LOG_TRACE("Succeed in getting tuple %s, RID %s in sequential scan.", tuple->ToString(&output_schema).c_str(),
              rid->ToString().c_str());
    return true;
  }

  return false;
}

auto SeqScanExecutor::RetrieveTuple(const Tuple &next_base_tuple, const TupleMeta &next_base_meta, const RID &next_rid,
                                    const Schema &schema) const -> Tuple {
  auto *transaction_manager = exec_ctx_->GetTransactionManager();
  const auto *transaction = exec_ctx_->GetTransaction();

  if (transaction->GetReadTs() > next_base_meta.ts_ || transaction->GetTransactionTempTs() == next_base_meta.ts_) {
    return next_base_tuple;
  }

  // Retrieve undo logs.
  auto optional_undo_link = transaction_manager->GetUndoLink(next_rid);
  std::vector<UndoLog> undo_logs;
  if (optional_undo_link.has_value()) {
    auto &undo_link = optional_undo_link.value();
    do {
      const auto undo_log = transaction_manager->GetUndoLog(undo_link);
      if (undo_log.ts_ < transaction->GetReadTs()) {
        undo_logs.push_back(undo_log);
      }
      undo_link = undo_log.prev_version_;
    } while (undo_link.IsValid());
  }

  const auto result_tuple = ReconstructTuple(&schema, next_base_tuple, next_base_meta, undo_logs);
  return result_tuple.has_value() ? result_tuple.value() : next_base_tuple;
}

}  // namespace bustub
