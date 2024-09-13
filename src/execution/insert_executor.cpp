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

#include "execution/executors/insert_executor.h"

#include <functional>
#include <memory>

#include "common/logger.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&GetOutputSchema()) {
  LOG_TRACE("Initialize insert executor.\n%s", plan_->ToString().c_str());
}

void InsertExecutor::Init() {
  child_executor_->Init();

  if (executor_result_.IsNotEmpty()) {
    executor_result_.SetOrResetBegin();
    return;
  }

  const auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  const auto indexes_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto *transaction = exec_ctx_->GetTransaction();

  int32_t inserted_rows_count = 0;
  RID child_rid;
  Tuple child_tuple;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    TupleMeta inserted_tuple_meta{transaction->GetTransactionTempTs(), false};

    // Insert tuple.
    if (const auto inserted_rid = table_info->table_->InsertTuple(inserted_tuple_meta, child_tuple); inserted_rid) {
      LOG_TRACE("Insert new entry: RID %s, tuple %s", inserted_rid->ToString().c_str(),
                child_tuple.ToString(&child_executor_->GetOutputSchema()).c_str());

      // Insert to write set and version chain of transaction.
      transaction->AppendWriteSet(plan_->table_oid_, *inserted_rid);

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

  executor_result_.EmplaceBack(Tuple({Value{INTEGER, inserted_rows_count}}, &GetOutputSchema()));
  executor_result_.SetOrResetBegin();
}

auto InsertExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  LOG_TRACE("Insert executor Next");
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    return true;
  }
  return false;
}

}  // namespace bustub
