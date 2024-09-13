//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/aggregation_executor.h"

#include <memory>
#include <vector>

#include "common/logger.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&GetOutputSchema()) {
  LOG_TRACE("Initialize aggregation executor.\n%s", plan_->ToString().c_str());
}

void AggregationExecutor::Init() {
  child_executor_->Init();

  if (executor_result_.IsNotEmpty()) {
    executor_result_.SetOrResetBegin();
    return;
  }

  // Get tuples from child executor and compute aggregations.
  Tuple child_tuple;
  RID rid;
  SimpleAggregationHashTable hash_table(plan_->aggregates_, plan_->agg_types_);
  while (child_executor_->Next(&child_tuple, &rid)) {
    const auto aggregate_key = MakeAggregateKey(&child_tuple);
    const auto aggregate_value = MakeAggregateValue(&child_tuple);
    hash_table.InsertCombine(aggregate_key, aggregate_value);
  }
  if (hash_table.Begin() == hash_table.End() && plan_->group_bys_.empty()) {
    // Generate empty default values.
    LOG_TRACE("Because the child executor is empty, need to add an empty aggregation value");
    const auto &[aggregates_] = hash_table.GenerateInitialAggregateValue();
    executor_result_.EmplaceBack({aggregates_});
  }

  // Generate aggregation results.
  auto hash_table_itr = hash_table.Begin();
  while (hash_table_itr != hash_table.End()) {
    const auto &group_bys = hash_table_itr.Key().group_bys_;
    const auto &aggregates = hash_table_itr.Val().aggregates_;
    ++hash_table_itr;

    executor_result_.EmplaceBack({group_bys.empty() ? std::vector<Value>() : group_bys, aggregates});
  }
  executor_result_.SetOrResetBegin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    LOG_TRACE("Result %s is obtained in aggregation executor", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
