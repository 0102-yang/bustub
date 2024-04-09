#include "execution/executors/executor_result.h"
#include "type/value_factory.h"
#include <cstdint>

namespace bustub {

ExecutorResult::ExecutorResult(const Schema *output_schema) : output_schema_(output_schema) {}

void ExecutorResult::EmplaceBack(std::vector<Value> &&values) { results_.emplace_back(std::move(values)); }

void ExecutorResult::EmplaceBack(const std::vector<std::vector<Value>> &values_array) {
  std::vector<Value> tmp_values;
  for (const auto &values : values_array) {
    std::copy(values.begin(), values.end(), std::back_inserter(tmp_values));
  }
  EmplaceBack(std::move(tmp_values));
}

void ExecutorResult::EmplaceBack(const std::vector<std::pair<const Tuple *, const Schema *>> &tuples) {
  std::vector<Value> tmp_values;
  for (const auto &[tuple, output_schema] : tuples) {
    const auto columns = output_schema->GetColumnCount();
    for (uint32_t i = 0; i < columns; i++) {
      tmp_values.push_back(tuple ? tuple->GetValue(output_schema, i)
                                 : ValueFactory::GetNullValueByType(output_schema->GetColumn(i).GetType()));
    }
  }
  EmplaceBack(std::move(tmp_values));
}

bool ExecutorResult::IsNotEnd() const { return iterator_ != results_.cend(); }

auto ExecutorResult::NextTuple() -> Tuple { return {*iterator_++, output_schema_}; }

auto ExecutorResult::IsNotEmpty() const -> bool { return !results_.empty(); }

void ExecutorResult::SetOrResetBegin() { iterator_ = results_.cbegin(); }

}  // namespace bustub
