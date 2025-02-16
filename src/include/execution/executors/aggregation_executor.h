//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// Identification: src/include/execution/executors/aggregation_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "executor_result.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_expressions the aggregation expressions
   * @param agg_types the types of aggregations
   */
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> &agg_expressions,
                             const std::vector<AggregationType> &agg_types)
      : agg_expressions_{agg_expressions}, agg_types_{agg_types} {}

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue() const -> AggregateValue {
    std::vector<Value> values{};
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // Count start starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          // Others starts at null.
          values.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) const {
    for (uint32_t i = 0; i < agg_expressions_.size(); i++) {
      switch (agg_types_[i]) {
        case AggregationType::CountStarAggregate:
          result->aggregates_[i] = result->aggregates_[i].Add({INTEGER, 1});
          break;
        case AggregationType::CountAggregate:
          if (!input.aggregates_[i].IsNull()) {
            result->aggregates_[i] =
                result->aggregates_[i].IsNull() ? Value{INTEGER, 1} : result->aggregates_[i].Add({INTEGER, 1});
          }
          break;
        case AggregationType::SumAggregate:
          if (!input.aggregates_[i].IsNull() && input.aggregates_[i].CheckInteger()) {
            result->aggregates_[i] = result->aggregates_[i].IsNull() ? input.aggregates_[i]
                                                                     : result->aggregates_[i].Add(input.aggregates_[i]);
          }
          break;
        case AggregationType::MinAggregate:
          if (!input.aggregates_[i].IsNull() && result->aggregates_[i].CheckComparable(input.aggregates_[i])) {
            result->aggregates_[i] = result->aggregates_[i].IsNull() ? input.aggregates_[i]
                                                                     : result->aggregates_[i].Min(input.aggregates_[i]);
          }
          break;
        case AggregationType::MaxAggregate:
          if (!input.aggregates_[i].IsNull() && result->aggregates_[i].CheckComparable(input.aggregates_[i])) {
            result->aggregates_[i] = result->aggregates_[i].IsNull() ? input.aggregates_[i]
                                                                     : result->aggregates_[i].Max(input.aggregates_[i]);
          }
          break;
      }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    [[nodiscard]] auto Key() const -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    [[nodiscard]] auto Val() const -> const AggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) const -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) const -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() const -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() const -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, AggregateValue> ht_;
  /** The aggregate expressions that we have */
  const std::vector<AbstractExpressionRef> &agg_expressions_;  // NOLINT
  /** The types of aggregations that we have */
  const std::vector<AggregationType> &agg_types_;  // NOLINT
};

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX)
 * over the tuples produced by a child executor.
 */
class AggregationExecutor final : public AbstractExecutor {
 public:
  /**
   * Construct a new AggregationExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the aggregation
   * @param[out] rid The next tuple RID produced by the aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the aggregation */
  [[nodiscard]] auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Do not use or remove this function, otherwise you will get zero points. */
  [[nodiscard]] auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) const -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expression : plan_->GetGroupBys()) {
      keys.emplace_back(expression->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) const -> AggregateValue {
    std::vector<Value> values;
    for (const auto &expression : plan_->GetAggregates()) {
      values.emplace_back(expression->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {values};
  }

  /** The aggregation plan node */
  const AggregationPlanNode *plan_;

  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_executor_;

  ExecutorResult executor_result_;
};
}  // namespace bustub
