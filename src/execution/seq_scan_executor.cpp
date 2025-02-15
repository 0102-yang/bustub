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
  LOG_TRACE("Initialize sequential scan executor.\n%s", plan_->ToString().c_str());
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
  auto *txn_manager = exec_ctx_->GetTransactionManager();
  const auto *txn = exec_ctx_->GetTransaction();

  // If the tuple is visible to the transaction, return the tuple.
  if (IsTupleVisibleToTransaction(base_meta, txn)) {
    return base_meta.is_deleted_ ? std::nullopt : std::make_optional(base_tuple);
  }

  // The tuple in heap is invisible to the transaction, need retrieve undo logs.
  const auto logs = RetrieveUndoLogs(txn_manager, rid, txn->GetReadTs());

  if (!logs || logs->empty()) {
    if (base_meta.is_deleted_ || base_meta.ts_ > txn->GetReadTs()) {
      return std::nullopt;
    }
  }

  // Reconstruct previous tuple version.
  return ReconstructTuple(&schema, base_tuple, base_meta, logs.value());
}

auto SeqScanExecutor::IsTupleVisibleToTransaction(const TupleMeta &base_meta, const Transaction *txn) -> bool {
  return txn->GetReadTs() >= base_meta.ts_ || txn->GetTransactionTempTs() == base_meta.ts_;
}

auto SeqScanExecutor::RetrieveUndoLogs(TransactionManager *txn_manager, const RID &rid, const timestamp_t read_ts)
    -> std::optional<std::vector<UndoLog>> {
  if (const auto optional_link = txn_manager->GetUndoLink(rid); optional_link.has_value()) {
    const auto watermark = txn_manager->GetWatermark();
    std::vector<UndoLog> logs;
    UndoLink link = *optional_link;
    while (link.IsValid()) {
      auto log = txn_manager->GetUndoLog(link);
      link = log.prev_version_;
      logs.push_back(log);
      if (log.ts_ <= read_ts || log.ts_ < watermark) {
        return logs;
      }
    }
  }
  return std::nullopt;
}

}  // namespace bustub
