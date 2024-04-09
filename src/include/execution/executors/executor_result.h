#pragma once

#include "storage/table/tuple.h"

namespace bustub {

class ExecutorResult {
 public:
  explicit ExecutorResult(const Schema *output_schema);

  ~ExecutorResult() = default;

  DISALLOW_COPY_AND_MOVE(ExecutorResult)

  void EmplaceBack(const std::vector<std::vector<Value>> &values_array);

  void EmplaceBack(const std::vector<std::pair<const Tuple *, const Schema *>> &tuples);

  auto NextTuple() -> Tuple;

  [[nodiscard]] bool IsNotEnd() const;

  [[nodiscard]] auto IsNotEmpty() const -> bool;

  void SetOrResetBegin();

 private:
  std::vector<std::vector<Value>> results_;

  std::vector<std::vector<Value>>::const_iterator iterator_;

  const Schema *output_schema_;

  void EmplaceBack(std::vector<Value> &&values);
};

}  // namespace bustub
