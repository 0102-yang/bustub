#pragma once

#include <vector>

#include "common/macros.h"
#include "storage/table/tuple.h"

namespace bustub {

class ExecutorResult {
 public:
  /**
   * @brief Constructs an ExecutorResult object with the given output schema.
   *
   * @param output_schema The output schema for the ExecutorResult.
   */
  explicit ExecutorResult(const Schema *output_schema);

  /**
   * @brief Destructor for the ExecutorResult object.
   */
  ~ExecutorResult() = default;

  DISALLOW_COPY_AND_MOVE(ExecutorResult)

  /**
   * @brief Emplace a new tuple into the ExecutorResult using the provided values array.
   *
   * @param values_array The values array representing the tuple.
   */
  void EmplaceBack(const std::vector<std::vector<Value>> &values_array);

  /**
   * @brief Emplace a new tuple into the ExecutorResult using the provided tuples.
   *
   * @param tuples The tuples to be emplace.
   */
  void EmplaceBack(const std::vector<std::pair<const Tuple *, const Schema *>> &tuples);

  /**
   * @brief Emplace a new tuple into the ExecutorResult using the provided tuple.
   *
   * @param tuple The tuple to be emplace.
   */
  void EmplaceBack(Tuple &&tuple);

  /**
   * @brief Retrieves the next tuple from the ExecutorResult.
   *
   * @return The next tuple.
   */
  auto Next() -> Tuple;

  /**
   * @brief Checks if the iterator has reached the end of the ExecutorResult.
   *
   * @return True if the iterator has not reached the end, false otherwise.
   */
  [[nodiscard]] auto IsNotEnd() const -> bool;

  /**
   * @brief Returns the number of tuples in the ExecutorResult.
   *
   * @return The number of tuples.
   */
  [[nodiscard]] auto Size() const -> size_t { return results_.size(); }

  /**
   * @brief Checks if the ExecutorResult is not empty.
   *
   * @return True if the ExecutorResult is not empty, false otherwise.
   */
  [[nodiscard]] auto IsNotEmpty() const -> bool;

  /**
   * @brief Reverses the order of tuples in the ExecutorResult.
   */
  void Reverse();

  /**
   * @brief Sets or resets the iterator to the beginning of the ExecutorResult.
   */
  void SetOrResetBegin();

 private:
  std::vector<Tuple> results_;                  /**< The vector of tuples in the ExecutorResult. */
  std::vector<Tuple>::const_iterator iterator_; /**< The iterator for the ExecutorResult. */
  const Schema *output_schema_;                 /**< The output schema for the ExecutorResult. */
};

}  // namespace bustub
