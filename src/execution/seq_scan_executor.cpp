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

#include <optional>

#include <catalog/schema.h>
#include <common/rid.h>
#include <concurrency/transaction.h>
#include <execution/executor_context.h>
#include <execution/executors/abstract_executor.h>
#include <execution/plans/seq_scan_plan.h>
#include <storage/table/tuple.h>
#include <type/value.h>
#include "common/logger.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iterator_(MakeIterator()) {
  LOG_DEBUG("Initialize sequential scan executor.\n%s", plan_->ToString().c_str());
}

void SeqScanExecutor::Init() { table_iterator_ = MakeIterator(); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto &output_schema = GetOutputSchema();
  const auto &predicate = plan_->filter_predicate_;

  while (!table_iterator_.IsEnd()) {
    // Get next tuple.
    const auto [base_meta, base_tuple] = table_iterator_.GetTuple();
    *rid = table_iterator_.GetRID();
    ++table_iterator_;

    if (predicate && !predicate->Evaluate(&base_tuple, output_schema).GetAs<bool>()) {
      LOG_TRACE("Failed to get tuple %s due to dissatisfy conditions",
                next_base_tuple.ToString(&output_schema).c_str());
      continue;
    }

    // Get reconstructed tuple.
    const auto reconstructed_tuple =
        ReconstructTupleFromTableHeapAndUndoLogs(base_tuple, base_meta, *rid, output_schema);
    if (!reconstructed_tuple.has_value()) {
      continue;
    }

    *tuple = *reconstructed_tuple;
    LOG_TRACE("Succeed in getting tuple %s, RID %s in sequential scan.", tuple->ToString(&output_schema).c_str(),
              rid->ToString().c_str());
    return true;
  }

  return false;
}

auto SeqScanExecutor::ReconstructTupleFromTableHeapAndUndoLogs(const Tuple &base_tuple, const TupleMeta &base_meta,
                                                               const RID &rid, const Schema &schema) const
    -> std::optional<Tuple> {
  auto *transaction_manager = exec_ctx_->GetTransactionManager();
  const auto *transaction = exec_ctx_->GetTransaction();

  // If the tuple is visible to the transaction, return the tuple.
  if (transaction->GetReadTs() >= base_meta.ts_ || transaction->GetTransactionTempTs() == base_meta.ts_) {
    return base_meta.is_deleted_ ? std::nullopt : std::make_optional(base_tuple);
  }

  // The tuple in heap is invisible to the transaction, need retrieve undo logs.
  const auto optional_link = transaction_manager->GetUndoLink(rid);
  std::vector<UndoLog> undo_logs;
  if (optional_link.has_value()) {
    UndoLink link = *optional_link;
    while (link.IsValid()) {
      auto log = transaction_manager->GetUndoLog(link);
      link = log.prev_version_;
      if (log.ts_ <= transaction->GetReadTs()) {
        undo_logs.push_back(std::move(log));
      }
    }
  }

  // If there are no undo logs, return null.
  if (undo_logs.empty()) {
    return std::nullopt;
  }
  return ReconstructTuple(&schema, base_tuple, base_meta, undo_logs);
}

}  // namespace bustub
