#include <algorithm>
#include <memory>

#include "common/logger.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equal-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  // Recursively optimizes the children of plan to hash join plan node.
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // Checks if the optimized plan is a nested loop join plan node.
  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    LOG_TRACE("Failed to optimize %s to hash join plan due to plan is not a nested loop join plan",
              optimized_plan->ToString().c_str());
    return optimized_plan;
  }

  /**
   * Checks if the given expression is a conjunction of equal-conditions.
   *
   * @param expression The expression to check.
   * @return True if the expression is a conjunction of equal-conditions, false otherwise.
   */
  std::function<bool(const AbstractExpressionRef &)> is_conjunction_of_equal_conditions;
  /**
   * Fetches the key expressions from the given predicate.
   *
   * @param expression The expression to fetch key expressions from.
   * @param left_key_expressions The vector to store the left key expressions.
   * @param right_key_expressions The vector to store the right key expressions.
   */
  std::function<void(const AbstractExpressionRef &, std::vector<AbstractExpressionRef> &,
                     std::vector<AbstractExpressionRef> &)>
      fetch_key_expressions;
  is_conjunction_of_equal_conditions = [&](const AbstractExpressionRef &expression) {
    if (const auto comparison_expression = std::dynamic_pointer_cast<const ComparisonExpression>(expression);
        comparison_expression) {
      const auto left = comparison_expression->GetChildAt(0);
      const auto right = comparison_expression->GetChildAt(1);
      return is_conjunction_of_equal_conditions(left) && is_conjunction_of_equal_conditions(right);
    }

    if (const auto logic_expression = std::dynamic_pointer_cast<const LogicExpression>(expression); logic_expression) {
      bool flag = true;
      for (const auto &child : logic_expression->GetChildren()) {
        flag &= is_conjunction_of_equal_conditions(child);
        if (!flag) {
          return false;
        }
      }
      return true;
    }

    const auto column_value_expression = std::dynamic_pointer_cast<const ColumnValueExpression>(expression);
    return column_value_expression != nullptr;
  };
  fetch_key_expressions = [&](const AbstractExpressionRef &expression,
                              std::vector<AbstractExpressionRef> &left_key_expressions,
                              std::vector<AbstractExpressionRef> &right_key_expressions) {
    if (const auto comparison_expression = std::dynamic_pointer_cast<ComparisonExpression>(expression);
        comparison_expression) {
      const auto left = comparison_expression->GetChildAt(0);
      const auto right = comparison_expression->GetChildAt(1);
      fetch_key_expressions(left, left_key_expressions, right_key_expressions);
      fetch_key_expressions(right, left_key_expressions, right_key_expressions);
    }

    if (const auto logic_expression = std::dynamic_pointer_cast<LogicExpression>(expression); logic_expression) {
      for (const auto &child : logic_expression->GetChildren()) {
        fetch_key_expressions(child, left_key_expressions, right_key_expressions);
      }
    }

    if (const auto column_value_expression = std::dynamic_pointer_cast<ColumnValueExpression>(expression);
        column_value_expression) {
      column_value_expression->GetTupleIdx() == 0 ? left_key_expressions.push_back(column_value_expression)
                                                  : right_key_expressions.push_back(column_value_expression);
    }
  };

  /*
   * Optimizes the nested loop join plan node to hash join plan node.
   */

  const auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  const auto &predicate = nlj_plan.Predicate();

  // Checks if the predicate is a conjunction of equal-conditions.
  if (const auto flag = is_conjunction_of_equal_conditions(predicate); !flag) {
    LOG_TRACE(
        "Failed to optimize %s to hash join plan due to predicate is not a conjunction of "
        "equal-conditions",
        nlj_plan.ToString().c_str());
    return optimized_plan;
  }

  // Fetches the key expressions from the predicate.
  std::vector<AbstractExpressionRef> left_key_expressions;
  std::vector<AbstractExpressionRef> right_key_expressions;
  fetch_key_expressions(predicate, left_key_expressions, right_key_expressions);

  // Generate hash join plan.
  const auto hash_join_plan =
      std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                         left_key_expressions, right_key_expressions, nlj_plan.GetJoinType());
  LOG_TRACE("Optimized %s to %s\n", nlj_plan.ToString().c_str(), hash_join_plan->ToString().c_str());
  return hash_join_plan;
}

}  // namespace bustub
