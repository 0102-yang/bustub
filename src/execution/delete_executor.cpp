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

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&GetOutputSchema()) {
  LOG_TRACE("Initialize delete executor.\n%s", plan_->ToString().c_str());
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deletion_finish_) {
    return false;
  }

  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  const auto indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int32_t deleted_rows_count = 0;
  Tuple child_tuple;

  while (child_executor_->Next(&child_tuple, rid)) {
    TupleMeta new_deleted_tuple_meta;
    new_deleted_tuple_meta.is_deleted_ = true;

    table_info->table_->UpdateTupleMeta(new_deleted_tuple_meta, *rid);
    LOG_TRACE("Delete tuple %s, RID %s", child_tuple.ToString(&child_executor_->GetOutputSchema()).c_str(),
              rid->ToString().c_str());

    // Delete indexes.
    for (const auto index_info : indexes_info) {
      const auto key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                      index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, nullptr);
      LOG_TRACE("Delete index of RID %s from index %s", rid->ToString().c_str(),
                index_info->index_->ToString().c_str());
    }

    deleted_rows_count++;
  }

  is_deletion_finish_ = true;
  *tuple = Tuple({Value{INTEGER, deleted_rows_count}}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
