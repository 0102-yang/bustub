#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      executor_result_(&plan_->OutputSchema()) {
  LOG_DEBUG("Initialize top-n executor.\n%s", plan_->ToString().c_str());
}

void TopNExecutor::Init() {
  child_executor_->Init();

  if (executor_result_.IsNotEmpty()) {
    executor_result_.SetOrResetBegin();
    return;
  }

  const size_t n = plan_->GetN();
  const auto &order_bys = plan_->GetOrderBy();
  const auto &output_schema = plan_->OutputSchema();

  Tuple tuple;
  RID rid;
  std::vector<Tuple> top_tuples;

  /**
   * @brief Comparison function used for sorting tuples in the top-n executor.
   *
   * This function compares two tuples based on the specified order by expressions.
   * It iterates through each order by expression and evaluates the expressions on the tuples.
   * The comparison is performed based on the evaluated sort values.
   *
   * @param t1 The first tuple to compare.
   * @param t2 The second tuple to compare.
   * @return True if t1 should come before t2 in the sorted order, false otherwise.
   */
  const std::function<bool(const Tuple &, const Tuple &)> cmp = [&order_bys, &output_schema](const auto &t1,
                                                                                             const auto &t2) {
    for (const auto &[type, expression] : order_bys) {
      if (type == OrderByType::INVALID) {
        continue;
      }

      const auto sort_value1 = expression->Evaluate(&t1, output_schema);
      const auto sort_value2 = expression->Evaluate(&t2, output_schema);

      if (sort_value1.CompareLessThan(sort_value2) == CmpBool::CmpTrue) {
        return type != OrderByType::DESC;
      }
      if (sort_value1.CompareGreaterThan(sort_value2) == CmpBool::CmpTrue) {
        return type == OrderByType::DESC;
      }
    }
    return false;
  };

  while (child_executor_->Next(&tuple, &rid)) {
    top_tuples.push_back(tuple);
    std::push_heap(top_tuples.begin(), top_tuples.end(), cmp);

    if (top_tuples.size() > n) {
      std::pop_heap(top_tuples.begin(), top_tuples.end(), cmp);
      top_tuples.pop_back();
    }
  }

  while (!top_tuples.empty()) {
    std::pop_heap(top_tuples.begin(), top_tuples.end(), cmp);
    auto max_tuple = top_tuples.back();
    top_tuples.pop_back();
    executor_result_.EmplaceBack(std::move(max_tuple));
  }

  executor_result_.Reverse();
  executor_result_.SetOrResetBegin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (executor_result_.IsNotEnd()) {
    *tuple = executor_result_.Next();
    *rid = tuple->GetRid();
    LOG_TRACE("Result %s is obtained in top-n executor", tuple->ToString(&GetOutputSchema()).c_str());
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() const -> size_t { return executor_result_.Size(); }

}  // namespace bustub
