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
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  LOG_DEBUG("Aggregation plan node: %s", plan_->ToString().c_str());

  // Get tuples from child executor and compute aggregations.
  Tuple child_tuple;
  RID rid;
  const bool need_hash_table = !plan_->group_bys_.empty();
  SimpleAggregationHashTable hash_table(plan_->aggregates_, plan_->agg_types_);
  AggregateValue final_aggregate_value = hash_table.GenerateInitialAggregateValue();
  while (child_executor_->Next(&child_tuple, &rid)) {
    LOG_DEBUG("Retrive tuple %s, RID %s from child executor",
              child_tuple.ToString(&child_executor_->GetOutputSchema()).c_str(), rid.ToString().c_str());

    if (need_hash_table) {
      // Need hash table to aggregate.
      const auto aggregate_key = MakeAggregateKey(&child_tuple);
      const auto aggregate_value = MakeAggregateValue(&child_tuple);
      hash_table.InsertCombine(aggregate_key, aggregate_value);
    } else {
      // Don't need hash table to aggregate.
      const auto aggregate_value = MakeAggregateValue(&child_tuple);
      hash_table.CombineAggregateValues(&final_aggregate_value, aggregate_value);
    }
  }

  // Generate final aggregate key and values.
  if (need_hash_table) {
    LOG_DEBUG("Generate with external hash table");

    auto hash_table_itr = hash_table.Begin();
    while (hash_table_itr != hash_table.End()) {
      const auto &group_bys = hash_table_itr.Key().group_bys_;
      const auto &aggregates = hash_table_itr.Val().aggregates_;
      ++hash_table_itr;

      std::vector<Value> tuple_values;
      tuple_values.reserve(group_bys.size() + aggregates.size());
      std::copy(group_bys.begin(), group_bys.end(), std::back_inserter(tuple_values));
      std::copy(aggregates.begin(), aggregates.end(), std::back_inserter(tuple_values));

      final_aggregate_key_values_.push_back(tuple_values);
    }
  } else {
    LOG_DEBUG("Generate without external hash table");

    const auto &aggregates = final_aggregate_value.aggregates_;
    final_aggregate_key_values_.push_back(aggregates);
  }

  iterator_ = final_aggregate_key_values_.begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iterator_ != final_aggregate_key_values_.end()) {
    const auto &tuple_values = *iterator_++;
    *tuple = Tuple(tuple_values, &GetOutputSchema());
    LOG_DEBUG("Generate aggregation result %s", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
