#include "execution/executors/values_executor.h"
#include "common/logger.h"

namespace bustub {

ValuesExecutor::ValuesExecutor(ExecutorContext *exec_ctx, const ValuesPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), dummy_schema_(Schema({})) {
  LOG_TRACE("Initialize value executor.\n%s", plan_->ToString().c_str());
}

void ValuesExecutor::Init() { cursor_ = 0; }

auto ValuesExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= plan_->GetValues().size()) {
    return false;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  const auto &row_expr = plan_->GetValues()[cursor_];
  for (const auto &col : row_expr) {
    values.push_back(col->Evaluate(nullptr, dummy_schema_));
  }

  *tuple = Tuple{values, &GetOutputSchema()};
  cursor_ += 1;

  return true;
}

}  // namespace bustub
