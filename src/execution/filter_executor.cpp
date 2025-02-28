#include "execution/executors/filter_executor.h"
#include "common/logger.h"
#include "type/value_factory.h"

namespace bustub {

FilterExecutor::FilterExecutor(ExecutorContext *exec_ctx, const FilterPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  LOG_TRACE("Initialize filter executor.\n%s", plan_->ToString().c_str());
}

void FilterExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto FilterExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const auto filter_expr = plan_->GetPredicate();

  while (true) {
    // Get the next tuple
    if (const auto status = child_executor_->Next(tuple, rid); !status) {
      return false;
    }

    if (auto value = filter_expr->Evaluate(tuple, child_executor_->GetOutputSchema());
        !value.IsNull() && value.GetAs<bool>()) {
      return true;
    }
  }
}

}  // namespace bustub
