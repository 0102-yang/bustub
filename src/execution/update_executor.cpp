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
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  LOG_DEBUG("Initialize update executor.\n%s", plan_->ToString().c_str());
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_update_finish_) {
    return false;
  }

  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  const auto indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int32_t updated_rows_count = 0;
  Tuple child_tuple;

  while (child_executor_->Next(&child_tuple, rid)) {
    // Todo: Need to update tuples in place.
    // Delete affected tuple.
    LOG_TRACE("Delete old tuple");
    TupleMeta delete_tuple_meta;
    delete_tuple_meta.is_deleted_ = true;

    table_info->table_->UpdateTupleMeta(delete_tuple_meta, *rid);

    // Delete indexes.
    for (const auto index_info : indexes_info) {
      const auto key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                      index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, nullptr);
      LOG_TRACE("Delete index of RID %s from index %s", rid->ToString().c_str(),
                index_info->index_->ToString().c_str());
    }

    // Insert new tuple.
    LOG_TRACE("Insert new tuple");
    std::vector<Value> new_updated_tuple_values;
    for (const auto &expression : plan_->target_expressions_) {
      const auto value = expression->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
      LOG_TRACE("After performing the evaluation of expression %s, the result value is %s",
                expression->ToString().c_str(), value.ToString().c_str());
      new_updated_tuple_values.push_back(value);
    }

    Tuple new_updated_tuple(new_updated_tuple_values, &child_executor_->GetOutputSchema());
    TupleMeta new_updated_tuple_meta;
    new_updated_tuple_meta.is_deleted_ = false;
    new_updated_tuple_meta.ts_ = 0;
    const auto inserted_rid = table_info->table_->InsertTuple(new_updated_tuple_meta, new_updated_tuple);
    LOG_TRACE("Update tuple is %s", new_updated_tuple.ToString(&child_executor_->GetOutputSchema()).c_str());

    // Insert indexes.
    for (const auto index_info : indexes_info) {
      const auto key_tuple = new_updated_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                            index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key_tuple, *inserted_rid, nullptr);
      LOG_TRACE("Update new index of RID %s to index %s", inserted_rid->ToString().c_str(),
                index_info->index_->ToString().c_str());
    }

    updated_rows_count++;
  }

  is_update_finish_ = true;
  *tuple = Tuple({Value{INTEGER, updated_rows_count}}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
