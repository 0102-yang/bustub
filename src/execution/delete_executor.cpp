//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/delete_executor.h"

#include <memory>

#include "common/logger.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&GetOutputSchema()) {
  LOG_TRACE("Initialize delete executor.\n%s", plan_->ToString().c_str());
}

void DeleteExecutor::Init() {
  child_executor_->Init();

  if (executor_result_.IsNotEmpty()) {
    executor_result_.SetOrResetBegin();
    return;
  }

  // Retrieve tuples in child executor and put them into buffer.
  Tuple child_tuple;
  RID child_rid;
  std::vector<Tuple> tuples_buffer;
  std::vector<RID> rids_buffer;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_buffer.emplace_back(std::move(child_tuple));
    rids_buffer.emplace_back(child_rid);
  }

  auto *txn_manager = exec_ctx_->GetTransactionManager();
  auto *txn = exec_ctx_->GetTransaction();
  const auto table_oid = plan_->GetTableOid();
  const auto table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
  const auto *table_heap = table_info->table_.get();
  const auto indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  // Check write-write conflict.
  CheckWriteWriteConflict(txn, table_heap, rids_buffer);

  int32_t deleted_rows_count = 0;
  const auto &tuple_schema = child_executor_->GetOutputSchema();
  // Delete tuples.
  for (size_t i = 0; i < tuples_buffer.size(); i++) {
    const auto &tuple = tuples_buffer[i];
    const auto &rid = rids_buffer[i];

    // Generate undo log.
    if (const auto [ts_, is_deleted_] = table_heap->GetTupleMeta(rid); ts_ < txn->GetTransactionId()) {
      // This tuple is firstly modified by this transaction.
      const std::vector modified_fields(tuple_schema.GetColumnCount(), true);
      AppendAndLinkUndoLog(txn_manager, txn, table_oid, rid, {false, modified_fields, tuple, ts_, {}});
    }

    // Delete tuple and its indexes.
    // Delete tuple.
    table_heap->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, rid);
    LOG_TRACE("Delete tuple %s, RID %s", tuple.ToString(tuple_schema).c_str(), rid->ToString().c_str());

    // Delete indexes.
    for (const auto index_info : indexes_info) {
      const auto key_tuple =
          tuple.KeyFromTuple(tuple_schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, rid, nullptr);
      LOG_TRACE("Delete index of RID %s from index %s", rid->ToString().c_str(),
                index_info->index_->ToString().c_str());
    }

    deleted_rows_count++;
  }

  Tuple deleted_result_tuple({Value{INTEGER, deleted_rows_count}}, &GetOutputSchema());
  executor_result_.EmplaceBack(std::move(deleted_result_tuple));
  executor_result_.SetOrResetBegin();
}

auto DeleteExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  LOG_TRACE("Update executor Next");
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    return true;
  }
  return false;
}

}  // namespace bustub
