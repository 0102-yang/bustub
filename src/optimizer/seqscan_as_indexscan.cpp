#include "execution/expressions/column_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) const -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  if (plan->GetType() != PlanType::SeqScan) {
    return plan;
  }

  // Check filter predicate.
  const auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);
  LOG_DEBUG("Retrive %s plan", seq_scan_plan.ToString().c_str());
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    LOG_TRACE("Optimize failed, filter predicate is nullptr");
    return plan;
  }

  // Check the filtering columns from the predicate.
  if (seq_scan_plan.filter_predicate_->GetChildren().size() != 2 ||
      !seq_scan_plan.filter_predicate_->GetChildAt(0)->GetChildren().empty() ||
      !seq_scan_plan.filter_predicate_->GetChildAt(1)->GetChildren().empty()) {
    LOG_TRACE("Optimize failed, there are not two final children expressions of filter predicate");
    return plan;
  }

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
    LOG_DEBUG("Optimize succeed, seqscan node to indexscan node - Index Scan Node: %s", index_scan_plan->ToString().c_str());
    return index_scan_plan;
  }
  LOG_TRACE("Optimize failed, no matched index");
  return plan;
}

}  // namespace bustub
