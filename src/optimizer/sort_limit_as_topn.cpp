#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // Recursively optimizes the children of plan to index scan plan node.
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto cloned_plan = plan->CloneWithChildren(std::move(children));

  /* Optimize sort limit plan to top-n plan. */

  if (cloned_plan->GetType() != PlanType::Limit) {
    LOG_TRACE("Failed to optimize %s to top-n plan due to plan is not a limit plan.", cloned_plan->ToString().c_str());
    return cloned_plan;
  }
  const auto limit_plan = dynamic_cast<const LimitPlanNode &>(*cloned_plan);

  if (limit_plan.GetChildAt(0)->GetType() != PlanType::Sort) {
    LOG_TRACE("Failed to optimize %s to top-n plan due to plan does not have a child sort plan.",
              cloned_plan->ToString().c_str());
    return cloned_plan;
  }
  const auto sort_plan = dynamic_cast<const SortPlanNode &>(*cloned_plan->GetChildAt(0));
  const auto sort_children = sort_plan.GetChildAt(0);

  // Generate top-n plan node.
  const auto top_n_plan = std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_children,
                                                         sort_plan.GetOrderBy(), limit_plan.GetLimit());
  LOG_TRACE("Succeed in optimizing %s and %s to %s.", limit_plan.ToString().c_str(), sort_plan.ToString().c_str(),
            top_n_plan->ToString().c_str());
  return top_n_plan;
}

}  // namespace bustub
