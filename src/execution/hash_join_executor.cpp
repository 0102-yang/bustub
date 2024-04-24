//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

#include "common/util/hash_util.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      executor_result_(&GetOutputSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  LOG_DEBUG("Initialize hash join executor with %s", plan_->ToString().c_str());
}

void HashJoinExecutor::Init() {
  // Initialize.

  left_child_->Init();
  right_child_->Init();

  if (executor_result_.IsNotEmpty()) {
    executor_result_.SetOrResetBegin();
    return;
  }

  const auto left_join_expressions = plan_->LeftJoinKeyExpressions();
  const auto right_join_expressions = plan_->RightJoinKeyExpressions();
  const Schema *left_schema = &left_child_->GetOutputSchema();
  const Schema *right_schema = &right_child_->GetOutputSchema();

  /*
   * Generate hash tables.
   */

  constexpr size_t hash_table_size = 64;
  std::vector<std::vector<Tuple>> left_hash_table(hash_table_size);
  std::vector<std::vector<Tuple>> right_hash_table(hash_table_size);
  Tuple tmp_tuple;
  RID tmp_rid;

  // Build the left hash table.
  while (left_child_->Next(&tmp_tuple, &tmp_rid)) {
    const auto hash_key = Hash(&tmp_tuple, left_schema, left_join_expressions);
    left_hash_table[hash_key % hash_table_size].emplace_back(tmp_tuple);
  }

  // Build the right hash table.
  while (right_child_->Next(&tmp_tuple, &tmp_rid)) {
    const auto hash_key = Hash(&tmp_tuple, right_schema, right_join_expressions);
    right_hash_table[hash_key % hash_table_size].emplace_back(tmp_tuple);
  }

  /*
   * Generate executor result.
   */

  for (size_t i = 0; i < hash_table_size; i++) {
    const auto &left_bucket = left_hash_table[i];
    const auto &right_bucket = right_hash_table[i];

    for (const auto &left_tuple : left_bucket) {
      bool left_join_null = true;

      for (const auto &right_tuple : right_bucket) {
        // Compare keys.
        bool is_keys_equal = true;
        for (size_t j = 0; j < left_join_expressions.size(); j++) {
          if (const Value left_value = left_join_expressions[j]->Evaluate(&left_tuple, *left_schema),
              right_value = right_join_expressions[j]->Evaluate(&right_tuple, *right_schema);
              left_value.CompareNotEquals(right_value) == CmpBool::CmpTrue) {
            is_keys_equal = false;
            break;
          }
        }

        if (!is_keys_equal) {
          if (plan_->GetJoinType() == JoinType::INNER) {
            LOG_TRACE("The inner join is failed due to hash is wrong on left %s and right %s",
                      left_tuple.ToString(left_schema).c_str(), right_tuple.ToString(right_schema).c_str());
          }
          continue;
        }

        if (plan_->GetJoinType() == JoinType::LEFT) {
          left_join_null = false;
        }

        // Add tuple values to result.
        executor_result_.EmplaceBack({{&left_tuple, left_schema}, {&right_tuple, right_schema}});
        LOG_TRACE("Succeed in inner or left join. Added one result tuple to final results with left %s and right %s",
                  left_tuple.ToString(left_schema).c_str(), right_tuple.ToString(right_schema).c_str());
      }

      // Handle special case where the right tuple
      // of a left tuple in the left join does not
      // satisfy the predicate.
      if (plan_->GetJoinType() == JoinType::LEFT && left_join_null) {
        executor_result_.EmplaceBack({{&left_tuple, left_schema}, {nullptr, right_schema}});
        LOG_TRACE("For null left join, added one result tuple to final results");
      }
    }
  }
  executor_result_.SetOrResetBegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    LOG_TRACE("Result %s is obtained in hash loop join", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }
  return false;
}

auto HashJoinExecutor::Hash(const Tuple *tuple, const Schema *schema,
                            const std::vector<AbstractExpressionRef> &expressions) -> hash_t {
  hash_t hash = 0;
  for (const auto &expr : expressions) {
    const Value value = expr->Evaluate(tuple, *schema);
    hash = HashUtil::CombineHashes(hash, HashUtil::HashValue(&value));
  }
  return hash;
}

}  // namespace bustub
