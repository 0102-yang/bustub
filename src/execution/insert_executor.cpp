//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

#include <functional>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  LOG_DEBUG("Initialize insert executor with %s", plan_->ToString().c_str());
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  LOG_TRACE("Insert executor Next");

  if (is_insertion_finish_) {
    return false;
  }

  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  const auto indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int32_t inserted_rows_count = 0;
  Tuple child_tuple;

  while (child_executor_->Next(&child_tuple, rid)) {
    TupleMeta inserted_tuple_meta;
    inserted_tuple_meta.is_deleted_ = false;
    inserted_tuple_meta.ts_ = 0;

    // Insert tuple.
    if (const auto inserted_rid = table_info->table_->InsertTuple(inserted_tuple_meta, child_tuple); inserted_rid) {
      LOG_TRACE("Insert new entry: RID %s, tuple %s", inserted_rid->ToString().c_str(),
                child_tuple.ToString(&child_executor_->GetOutputSchema()).c_str());

      // Insert indexes.
      for (const auto index_info : indexes_info) {
        const auto key_tuple = child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_,
                                                        index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key_tuple, *inserted_rid, nullptr);
        LOG_TRACE("Insert new index of RID %s to index %s", inserted_rid->ToString().c_str(),
                  index_info->index_->ToString().c_str());
      }
      inserted_rows_count++;
    }
  }

  is_insertion_finish_ = true;  // Generate inserted rows count.
  *tuple = Tuple({Value{INTEGER, inserted_rows_count}}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
