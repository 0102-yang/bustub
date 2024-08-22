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
    const auto [next_base_meta, next_base_tuple] = table_iterator_.GetTuple();
    const auto next_rid = table_iterator_.GetRID();
    ++table_iterator_;

    if (predicate && !predicate->Evaluate(&next_base_tuple, output_schema).GetAs<bool>()) {
      LOG_TRACE("Failed to get tuple %s due to dissatisfy conditions",
                next_base_tuple.ToString(&output_schema).c_str());
      continue;
    }

    // Get reconstructed tuple.
    const auto reconstructed_tuple =
        ReconstructTupleFromPreviousVersion(next_base_tuple, next_base_meta, next_rid, output_schema);
    if (!reconstructed_tuple.has_value()) {
      continue;
    }

    *tuple = *reconstructed_tuple;
    *rid = next_rid;
    LOG_TRACE("Succeed in getting tuple %s, RID %s in sequential scan.", tuple->ToString(&output_schema).c_str(),
              rid->ToString().c_str());
    return true;
  }

  return false;
}

auto SeqScanExecutor::ReconstructTupleFromPreviousVersion(const Tuple &next_base_tuple, const TupleMeta &next_base_meta,
                                                   const RID &next_rid, const Schema &schema) const
    -> std::optional<Tuple> {
  auto *transaction_manager = exec_ctx_->GetTransactionManager();
  const auto *transaction = exec_ctx_->GetTransaction();

  if (transaction->GetReadTs() >= next_base_meta.ts_ || transaction->GetTransactionTempTs() == next_base_meta.ts_) {
    return next_base_tuple;
  }

  // Retrieve undo logs.
  // Todo: Can be replaced by UndoLog iterator.
  const auto optional_link = transaction_manager->GetUndoLink(next_rid);
  std::vector<UndoLog> undo_logs;
  if (optional_link.has_value()) {
    UndoLink link = *optional_link;
    while (link.IsValid()) {
      const auto log = transaction_manager->GetUndoLog(link);
      if (log.ts_ <= transaction->GetReadTs()) {
        undo_logs.push_back(log);
      }
      link = log.prev_version_;
    }
  }

  // If transaction is not committed and has no undo logs, just return null.
  if (undo_logs.empty()) {
    return std::nullopt;
  }
  return ReconstructTuple(&schema, next_base_tuple, next_base_meta, undo_logs);
}

}  // namespace bustub
