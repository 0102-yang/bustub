//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      executor_result_(&GetOutputSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  LOG_DEBUG("Initialize nested loop join executor.\n%s", plan_->ToString().c_str());
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();

  if (executor_result_.IsNotEmpty()) {
    executor_result_.SetOrResetBegin();
    return;
  }

  /*
   * Generate executor results.
   */

  Tuple left_tuple;
  Tuple right_tuple;
  RID unused_rid;
  const auto nested_loop_join_predicate = plan_->Predicate();
  const auto left_schema = left_executor_->GetOutputSchema();
  const auto right_schema = right_executor_->GetOutputSchema();
  while (left_executor_->Next(&left_tuple, &unused_rid)) {
    bool left_join_null = true;
    right_executor_->Init();

    while (right_executor_->Next(&right_tuple, &unused_rid)) {
      if (nested_loop_join_predicate &&
          !nested_loop_join_predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema)
               .GetAs<bool>()) {
        if (plan_->GetJoinType() == JoinType::INNER) {
          LOG_TRACE("The inner join is failed due to predicate is wrong on left %s and right %s",
                    left_tuple.ToString(&left_schema).c_str(), right_tuple.ToString(&right_schema).c_str());
        }
        continue;
      }

      if (plan_->GetJoinType() == JoinType::LEFT) {
        left_join_null = false;
      }
      // Add tuple values to result.
      executor_result_.EmplaceBack({{&left_tuple, &left_schema}, {&right_tuple, &right_schema}});
      LOG_TRACE("Succeed in inner join. Added one result tuple to final results with left %s and right %s",
                left_tuple.ToString(&left_schema).c_str(), right_tuple.ToString(&right_schema).c_str());
    }

    // Handle special case where the right tuple
    // of a left tuple in the left join does not
    // satisfy the predicate.
    if (plan_->GetJoinType() == JoinType::LEFT && left_join_null) {
      executor_result_.EmplaceBack({{&left_tuple, &left_schema}, {nullptr, &right_schema}});
      LOG_TRACE("For null left join, added one result tuple to final results");
    }
  }
  executor_result_.SetOrResetBegin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, [[maybe_unused]] RID *rid) -> bool {
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    LOG_TRACE("Result %s is obtained in nested loop join", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }
  return false;
}

}  // namespace bustub
