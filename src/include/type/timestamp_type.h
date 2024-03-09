//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// timestamp_type.h
//
// Identification: src/include/type/timestamp_type.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "type/value.h"

namespace bustub {

class TimestampType : public Type {
 public:
  static constexpr uint64_t K_USECS_PER_DATE = 86400000000UL;

  ~TimestampType() override = default;
  TimestampType();

  // Comparison functions
  [[nodiscard]] auto CompareEquals(const Value &left, const Value &right) const -> CmpBool override;
  [[nodiscard]] auto CompareNotEquals(const Value &left, const Value &right) const -> CmpBool override;
  [[nodiscard]] auto CompareLessThan(const Value &left, const Value &right) const -> CmpBool override;
  [[nodiscard]] auto CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool override;
  [[nodiscard]] auto CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool override;
  [[nodiscard]] auto CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool override;

  // Other mathematical functions
  [[nodiscard]] auto Min(const Value &left, const Value &right) const -> Value override;
  [[nodiscard]] auto Max(const Value &left, const Value &right) const -> Value override;

  [[nodiscard]] auto IsInlined(const Value &val) const -> bool override { return true; }

  // Debug
  [[nodiscard]] auto ToString(const Value &val) const -> std::string override;

  // Serialize this value into the given storage space
  void SerializeTo(const Value &val, char *storage) const override;

  // Deserialize a value of the given type from the given storage space.
  auto DeserializeFrom(const char *storage) const -> Value override;

  // Create a copy of this value
  [[nodiscard]] auto Copy(const Value &val) const -> Value override;

  [[nodiscard]] auto CastAs(const Value &val, TypeId type_id) const -> Value override;
};

}  // namespace bustub
