#include "execution/executors/window_function_executor.h"

#include <numeric>

#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&plan_->OutputSchema()) {
  LOG_DEBUG("Initialize window function executor.\n%s", plan_->ToString().c_str());
}

void WindowFunctionExecutor::Init() {
  /*
   * Initialize child executor.
   */

  child_executor_->Init();
  const auto child_schema = child_executor_->GetOutputSchema();

  /*
   * Scan the child executor and store the tuples in a dictionary.
   */

  Tuple tmp_tuple;
  RID tmp_rid;
  TupleDictionary dictionary;
  TupleOrders orders;
  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    dictionary.emplace(tmp_rid, tmp_tuple);
    orders.emplace_back(tmp_rid);
  }

  /*
   * Reorder original tuple order.
   */

  const auto itr = std::find_if(plan_->window_functions_.begin(), plan_->window_functions_.end(),
                                [](const auto &p) { return !p.second.order_by_.empty(); });

  if (itr != plan_->window_functions_.end()) {
    const WindowFunctionPlanNode::WindowFunction *window_function_ptr = &itr->second;
    std::sort(orders.begin(), orders.end(),
              [&dictionary, &child_schema, window_function_ptr](const RID &rid1, const RID &rid2) {
                const auto order_by = window_function_ptr->order_by_;
                for (const auto &[type, expression] : order_by) {
                  if (type == OrderByType::INVALID) {
                    continue;
                  }

                  const auto t1 = dictionary.at(rid1);
                  const auto t2 = dictionary.at(rid2);
                  const auto s1 = expression->Evaluate(&t1, child_schema);
                  const auto s2 = expression->Evaluate(&t2, child_schema);

                  if (s1.CompareLessThan(s2) == CmpBool::CmpTrue) {
                    return type != OrderByType::DESC;
                  }
                  if (s1.CompareGreaterThan(s2) == CmpBool::CmpTrue) {
                    return type == OrderByType::DESC;
                  }
                }
                return false;
              });
  }

  /*
   * For each window function, we need to create a unique partition table.
   */
  const auto &window_functions = plan_->window_functions_;
  PartitionTables tables;
  for (const auto &[column_idx, window_function] : window_functions) {
    tables.emplace(column_idx, PartitionTable(&window_function, &dictionary, &child_schema));
  }

  /*
   * For each partition table, we need to insert the tuples.
   */
  for (auto &[column, table] : tables) {
    for (const auto &[rid, tuple] : dictionary) {
      table.Insert(tuple);
    }
  }

  /*
   * Generate result for each partition table.
   */

  for (auto &[_, table] : tables) {
    table.GenerateResult();
  }

  /*
   * Generate result tuples.
   */

  const auto &columns = plan_->columns_;
  const auto schema = plan_->OutputSchema();

  for (size_t rid_idx = 0; rid_idx < orders.size(); rid_idx++) {
    const auto &rid = orders.at(rid_idx);
    const auto &original_tuple = dictionary.at(rid);

    std::vector<Value> tuple_values;
    tuple_values.reserve(schema.GetColumnCount());

    // Generate result.
    for (uint32_t column_idx = 0; column_idx < columns.size(); column_idx++) {
      Value value;

      if (tables.find(column_idx) == tables.end()) {
        // Generate value with original tuple.
        const auto &column = columns.at(column_idx);
        value = column->Evaluate(&original_tuple, child_schema);
      } else {
        // Generate value with partition aggregate value or rank value.
        const auto &table = tables.at(column_idx);
        value = table.GetResult(rid_idx);
      }
      tuple_values.emplace_back(std::move(value));
    }

    // Save final result tuple.
    Tuple result_tuple(tuple_values, &schema);
    result_tuple.SetRid(rid);
    executor_result_.EmplaceBack(std::move(result_tuple));
  }
  executor_result_.SetOrResetBegin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    *rid = tuple->GetRid();
    LOG_TRACE("Result %s is obtained in window function executor", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }
  return false;
}

/*
 * Partition table.
 */

void WindowFunctionExecutor::PartitionTable::Insert(const Tuple &tuple) {
  const auto key = GenerateKey(tuple);

  auto it = partitions_.find(key);
  if (it == partitions_.end()) {
    partitions_.emplace(key, Rids());
    it = partitions_.find(key);
  }

  Rids &rids = it->second;
  RID rid = tuple.GetRid();
  rids.emplace_back(rid);
}

auto WindowFunctionExecutor::PartitionTable::GetResult(const size_t result_idx) const -> Value {
  return results_.at(result_idx);
}

auto WindowFunctionExecutor::PartitionTable::GenerateKey(const Tuple &tuple) const -> PartitionKey {
  const auto &partition_by = window_function_->partition_by_;

  // Generate key.
  PartitionKey key;
  for (const auto &expr : partition_by) {
    key.push_back(expr->Evaluate(&tuple, *schema_));
  }
  return key;
}

void WindowFunctionExecutor::PartitionTable::GenerateResult() {
  /*
   * For each partition, sort the tuples by the order by clause.
   */

  const auto sort = [this](const RID &rid1, const RID &rid2) {
    const auto order_by = window_function_->order_by_;
    for (const auto &[type, expression] : order_by) {
      if (type == OrderByType::INVALID) {
        continue;
      }

      const auto t1 = dictionary_->at(rid1);
      const auto t2 = dictionary_->at(rid2);
      const auto s1 = expression->Evaluate(&t1, *schema_);
      const auto s2 = expression->Evaluate(&t2, *schema_);

      if (s1.CompareLessThan(s2) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (s1.CompareGreaterThan(s2) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return false;
  };

  for (auto &[key, rids] : partitions_) {
    std::sort(rids.begin(), rids.end(), sort);
  }

  /*
   * Generate result for each partition.
   */

  const auto &function = window_function_->function_;
  const bool is_ordered_by = !window_function_->order_by_.empty();

  for (const auto &[key, rids] : partitions_) {
    switch (window_function_->type_) {
      case WindowFunctionType::CountStarAggregate: {
        if (is_ordered_by) {
          auto result = ValueFactory::GetIntegerValue(0);
          results_.reserve(rids.size());
          for (size_t i = 0; i < rids.size(); i++) {
            result = result.Add(ValueFactory::GetIntegerValue(1));
            results_.emplace_back(result);
          }
        } else {
          auto result = ValueFactory::GetIntegerValue(static_cast<int32_t>(rids.size()));
          for (size_t i = 0; i < rids.size(); i++) {
            results_.emplace_back(result);
          }
        }

        break;
      }
      case WindowFunctionType::CountAggregate: {
        if (is_ordered_by) {
          auto result = ValueFactory::GetIntegerValue(0);

          for (const auto &rid : rids) {
            const auto &tuple = dictionary_->at(rid);
            if (const auto value = function->Evaluate(&tuple, *schema_); !value.IsNull()) {
              result = result.Add(ValueFactory::GetIntegerValue(1));
            }
            results_.emplace_back(result);
          }
        } else {
          const auto count = std::count_if(rids.begin(), rids.end(), [this, &function](const RID &rid) {
            const auto &tuple = dictionary_->at(rid);
            const auto value = function->Evaluate(&tuple, *schema_);
            return !value.IsNull();
          });

          auto result = ValueFactory::GetIntegerValue(static_cast<int32_t>(count));
          for (size_t i = 0; i < rids.size(); i++) {
            results_.emplace_back(result);
          }
        }

        break;
      }
      case WindowFunctionType::MaxAggregate: {
        auto result = Type::GetMinValue(INTEGER);

        for (const auto &rid : rids) {
          const auto &tuple = dictionary_->at(rid);

          if (const auto value = function->Evaluate(&tuple, *schema_);
              !value.IsNull() && result.CheckComparable(value)) {
            result = result.Max(value);
          }

          if (is_ordered_by) {
            results_.emplace_back(result);
          }
        }

        if (!is_ordered_by) {
          for (size_t i = 0; i < rids.size(); i++) {
            results_.emplace_back(result);
          }
        }

        break;
      }
      case WindowFunctionType::MinAggregate: {
        auto result = Type::GetMaxValue(INTEGER);

        for (const auto &rid : rids) {
          const auto &tuple = dictionary_->at(rid);

          if (const auto value = function->Evaluate(&tuple, *schema_);
              !value.IsNull() && result.CheckComparable(value)) {
            result = result.Min(value);
          }

          if (is_ordered_by) {
            results_.emplace_back(result);
          }
        }

        if (!is_ordered_by) {
          for (size_t i = 0; i < rids.size(); i++) {
            results_.emplace_back(result);
          }
        }

        break;
      }
      case WindowFunctionType::SumAggregate: {
        auto result = ValueFactory::GetIntegerValue(0);

        for (const auto &rid : rids) {
          const auto &tuple = dictionary_->at(rid);

          if (const auto value = function->Evaluate(&tuple, *schema_); !value.IsNull()) {
            result = result.Add(value);
          }

          if (is_ordered_by) {
            results_.emplace_back(result);
          }
        }

        if (!is_ordered_by) {
          for (size_t i = 0; i < rids.size(); i++) {
            results_.emplace_back(result);
          }
        }

        break;
      }
      case WindowFunctionType::Rank: {
        const auto compare_tuples = [this](const Tuple &t1, const Tuple &t2) {
          for (uint32_t i = 0; i < schema_->GetColumnCount(); i++) {
            if (const auto value1 = t1.GetValue(schema_, i), value2 = t2.GetValue(schema_, i);
                value1.CompareEquals(value2) == CmpBool::CmpFalse) {
              return false;
            }
          }
          return true;
        };

        std::vector<unsigned> rank_result(rids.size());
        rank_result[0] = 1U;

        for (auto rank_diff = 1U, i = 1U; i < rank_result.size(); i++) {
          if (const auto &current_tuple = dictionary_->at(rids[i]), &last_tuple = dictionary_->at(rids[i - 1]);
              compare_tuples(current_tuple, last_tuple)) {
            rank_result[i] = rank_result[i - 1];
            rank_diff++;
          } else {
            rank_result[i] = rank_result[i - 1] + rank_diff;
            rank_diff = 1U;
          }
        }

        for (const auto &rank : rank_result) {
          results_.emplace_back(ValueFactory::GetIntegerValue(static_cast<int32_t>(rank)));
        }

        break;
      }
    }
  }
}

}  // namespace bustub
