//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/update_executor.h"

#include <memory>

#include "common/logger.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&GetOutputSchema()) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  LOG_TRACE("Initialize update executor.\n%s", plan_->ToString().c_str());
}

void UpdateExecutor::Init() {
  child_executor_->Init();

  if (executor_result_.IsNotEmpty()) {
    executor_result_.SetOrResetBegin();
    return;
  }

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_manager = exec_ctx_->GetTransactionManager();
  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  const auto *table_heap = table_info->table_.get();

  // Retrieve tuples in child executor and put them into buffer.
  Tuple child_tuple;
  RID child_rid;
  std::vector<Tuple> tuples_buffer;
  std::vector<RID> rids_buffer;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    tuples_buffer.emplace_back(std::move(child_tuple));
    rids_buffer.emplace_back(child_rid);
  }

  // Check write-write conflict.
  CheckWriteWriteConflict(txn, table_heap, rids_buffer);

  // Update tuples.
  int32_t updated_rows_count = 0;
  const auto &tuple_schema = child_executor_->GetOutputSchema();
  for (size_t tuple_idx = 0; tuple_idx < tuples_buffer.size(); tuple_idx++) {
    const auto &old_tuple = tuples_buffer[tuple_idx];
    const auto &rid = rids_buffer[tuple_idx];
    const auto [ts_, is_deleted_] = table_heap->GetTupleMeta(rid);

    // Generate modified fields.
    std::unordered_map<size_t, Value> updated_row_old_values;
    std::vector<bool> modified_fields;
    std::vector<Value> old_tuple_values;
    std::vector<Value> new_tuple_values;
    for (size_t expr_idx = 0; expr_idx < plan_->target_expressions_.size(); expr_idx++) {
      const auto &expression = plan_->target_expressions_[expr_idx];

      const auto old_value = old_tuple.GetValue(&tuple_schema, static_cast<uint32_t>(expr_idx));
      const auto new_value = expression->Evaluate(&old_tuple, tuple_schema);

      const bool is_row_updated = !old_value.CompareExactlyEquals(new_value);
      modified_fields.push_back(is_row_updated);
      new_tuple_values.push_back(new_value);
      if (is_row_updated) {
        old_tuple_values.push_back(old_value);
        updated_row_old_values.emplace(expr_idx, old_value);
      }
    }

    // Generate updated tuple.
    Tuple new_tuple{new_tuple_values, &tuple_schema};
    new_tuple.SetRid(rid);
    if (IsTupleContentEqual(old_tuple, new_tuple)) {
      continue;
    }

    // Update tuple in place.
    // Update undo log.
    if (ts_ == txn->GetTransactionId()) {
      // This tuple was previously modified by this transaction.
      TryUpdateUndoLog(txn, rid, tuple_schema, updated_row_old_values);
    } else {
      // This tuple is firstly modified by this transaction.
      const auto log_schema = GetUndoLogSchema(tuple_schema, modified_fields);
      Tuple log_tuple{old_tuple_values, &log_schema};
      log_tuple.SetRid(rid);
      AppendAndLinkUndoLog(txn_manager, txn, plan_->GetTableOid(), rid, {false, modified_fields, log_tuple, ts_, {}});
    }

    // Update tuple.
    table_heap->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, new_tuple, rid, nullptr);
    updated_rows_count++;
    LOG_TRACE("Update tuple %s, RID %s from transaction %s", updated_tuple.ToString(&schema).c_str(),
              rid.ToString().c_str(), txn->GetTransactionId());
  }

  Tuple updated_result_tuple({Value{INTEGER, updated_rows_count}}, &GetOutputSchema());
  executor_result_.EmplaceBack(std::move(updated_result_tuple));
  executor_result_.SetOrResetBegin();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  LOG_TRACE("Update executor Next");
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    return true;
  }
  return false;
}

}  // namespace bustub
