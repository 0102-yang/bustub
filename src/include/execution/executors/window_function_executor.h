//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "executor_result.h"
#include "storage/table/tuple.h"

namespace std {

using namespace bustub;

template <>
struct equal_to<std::vector<Value>> {
  bool operator()(const std::vector<Value> &lhs, const std::vector<Value> &rhs) const noexcept {
    if (lhs.size() != rhs.size()) {
      return false;
    }

    for (size_t i = 0; i < lhs.size(); i++) {
      if (lhs[i].CompareEquals(rhs[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

template <>
struct hash<std::vector<Value>> {
  std::size_t operator()(const std::vector<Value> &vec) const noexcept {
    size_t curr_hash = 0;
    for (const auto &value : vec) {
      if (!value.IsNull()) {
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&value));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputting rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functions and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor final : public AbstractExecutor {
  class PartitionTable;

 public:
  using PartitionTables = std::unordered_map<uint32_t, PartitionTable>;
  using TupleDictionary = std::unordered_map<RID, Tuple>;
  using TupleOrders = std::vector<RID>;

  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   * @param child_executor The child executor
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  [[nodiscard]] auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  class PartitionTable {
   public:
    PartitionTable(const WindowFunctionPlanNode::WindowFunction *window_function, const TupleDictionary *dictionary,
                   const Schema *schema)
        : window_function_(window_function), dictionary_(dictionary), schema_(schema) {}

    void Insert(const Tuple &tuple);

    auto GetResult(size_t result_idx) const -> Value;

    void GenerateResult();

   private:
    using PartitionKey = std::vector<Value>;
    using Rids = std::vector<RID>;

    auto GenerateKey(const Tuple &tuple) const -> PartitionKey;

    const WindowFunctionPlanNode::WindowFunction *window_function_;

    const TupleDictionary *dictionary_;

    const Schema *schema_;

    std::unordered_map<PartitionKey, Rids> partitions_;

    std::vector<Value> results_;
  };

  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  ExecutorResult executor_result_;
};
}  // namespace bustub
