//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor final : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  [[nodiscard]] auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;

  /** The table iterator. */
  TableIterator table_iterator_;

  [[nodiscard]] auto MakeIterator() const -> TableIterator {
    const auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
    return table_info->table_->MakeIterator();
  }

  [[nodiscard]] auto ReconstructTupleFromTableHeapAndUndoLogs(const Tuple &base_tuple, const TupleMeta &base_meta,
                                                              const RID &rid,
                                                              const Schema &schema) const -> std::optional<Tuple>;

  [[nodiscard]] static auto IsTupleVisibleToTransaction(const TupleMeta &base_meta, const Transaction *txn) -> bool;

  [[nodiscard]] static auto RetrieveUndoLogs(TransactionManager *txn_manager, const RID &rid,
                                             timestamp_t read_ts) -> std::optional<std::vector<UndoLog>>;
};
}  // namespace bustub
