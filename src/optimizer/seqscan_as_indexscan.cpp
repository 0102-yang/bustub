#include "execution/expressions/column_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) const -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Push Down has been enabled for you in optimizer.cpp when forcing starter rule

  // Recursively optimizes the children of plan to index scan plan node.
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // Checks if the optimized plan is a seq scan plan node.
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    LOG_TRACE("Failed to optimize %s to seq-scan plan due to plan is not a seq-scan plan.",
              optimized_plan->ToString().c_str());
    return optimized_plan;
  }

  // Checks if the filter predicate is nullptr.
  const auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    LOG_TRACE("Failed to optimize %s to index-scan plan due to filter predicate is nullptr",
              seq_scan_plan.ToString().c_str());
    return optimized_plan;
  }

  // Checks if the filter predicate satisfies the index-scan conditions.
  if (seq_scan_plan.filter_predicate_->GetChildren().size() != 2 ||
      !seq_scan_plan.filter_predicate_->GetChildAt(0)->GetChildren().empty() ||
      !seq_scan_plan.filter_predicate_->GetChildAt(1)->GetChildren().empty()) {
    LOG_TRACE(
        "Failed to optimize %s to index-scan plan due to there are not two final children expressions of filter "
        "predicate",
        seq_scan_plan.ToString().c_str());
    return optimized_plan;
  }

  /*
   * Optimizes the seq scan plan node to an index scan plan node.
   */

  const auto indexes_info = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
  const auto column_value_expr =
      dynamic_cast<const ColumnValueExpression *>(seq_scan_plan.filter_predicate_->GetChildAt(0).get());
  const auto constant_value_expr =
      dynamic_cast<ConstantValueExpression *>(seq_scan_plan.filter_predicate_->GetChildAt(1).get());

  if (const auto index_metadata = MatchIndex(seq_scan_plan.table_name_, column_value_expr->GetColIdx());
      index_metadata) {
    // Generate IndexScanPlanNode.
    const auto index_scan_plan = std::make_shared<IndexScanPlanNode>(
        seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, std::get<0>(index_metadata.value()),
        seq_scan_plan.filter_predicate_, constant_value_expr);
    LOG_TRACE("Succeed in optimizing %s to %s", seq_scan_plan.ToString().c_str(), index_scan_plan->ToString().c_str());
    return index_scan_plan;
  }
  LOG_TRACE("Failed to optimize %s to index-scan plan due to there is no matched index",
            seq_scan_plan.ToString().c_str());
  return optimized_plan;
}

}  // namespace bustub
