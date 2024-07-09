#include "execution/executors/sort_executor.h"
#include "common/logger.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&plan_->OutputSchema()) {
  LOG_DEBUG("Initialize sort executor.\n%s", plan_->ToString().c_str());
}

void SortExecutor::Init() {
  child_executor_->Init();

  const auto order_bys = plan_->GetOrderBy();
  const auto output_schema = plan_->OutputSchema();

  Tuple tuple;
  RID rid;
  std::vector<Tuple> sort_tuples;

  while (child_executor_->Next(&tuple, &rid)) {
    sort_tuples.push_back(tuple);
  }

  /**
   * Sort the tuples based on the specified order by expressions.
   *
   * The sorting is done in ascending order for each expression, unless the order by type is DESC.
   *
   * @param t1 The first tuple to compare.
   * @param t2 The second tuple to compare.
   * @return True if t1 should come before t2 in the sorted order, false otherwise.
   */
  std::sort(sort_tuples.begin(), sort_tuples.end(), [&order_bys, &output_schema](const auto &t1, const auto &t2) {
    for (const auto &[type, expression] : order_bys) {
      if (type == OrderByType::INVALID) {
        continue;
      }

      const auto s1 = expression->Evaluate(&t1, output_schema);
      const auto s2 = expression->Evaluate(&t2, output_schema);

      if (s1.CompareLessThan(s2) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (s1.CompareGreaterThan(s2) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return false;
  });

  for (auto &&sort_tuple : sort_tuples) {
    executor_result_.EmplaceBack(std::move(sort_tuple));
  }
  executor_result_.SetOrResetBegin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    *rid = tuple->GetRid();
    LOG_TRACE("Result %s is obtained in sort executor", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }
  return false;
}

}  // namespace bustub
