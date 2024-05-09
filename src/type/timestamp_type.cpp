//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// timestamp_type.cpp
//
// Identification: src/type/timestamp_type.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "type/timestamp_type.h"
#include <string>

#include "type/boolean_type.h"
#include "type/value_factory.h"

namespace bustub {

TimestampType::TimestampType() : Type(TIMESTAMP) {}

auto TimestampType::CompareEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return GetCmpBool(left.GetAs<uint64_t>() == right.GetAs<uint64_t>());
}

auto TimestampType::CompareNotEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return GetCmpBool(left.GetAs<uint64_t>() != right.GetAs<uint64_t>());
}

auto TimestampType::CompareLessThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return GetCmpBool(left.GetAs<uint64_t>() < right.GetAs<uint64_t>());
}

auto TimestampType::CompareLessThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return GetCmpBool(left.GetAs<uint64_t>() <= right.GetAs<uint64_t>());
}

auto TimestampType::CompareGreaterThan(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return GetCmpBool(left.GetAs<int64_t>() > right.GetAs<int64_t>());
}

auto TimestampType::CompareGreaterThanEquals(const Value &left, const Value &right) const -> CmpBool {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return CmpBool::CmpNull;
  }
  return GetCmpBool(left.GetAs<uint64_t>() >= right.GetAs<uint64_t>());
}

auto TimestampType::Min(const Value &left, const Value &right) const -> Value {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }
  if (left.CompareLessThan(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

auto TimestampType::Max(const Value &left, const Value &right) const -> Value {
  assert(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) {
    return left.OperateNull(right);
  }
  if (left.CompareGreaterThanEquals(right) == CmpBool::CmpTrue) {
    return left.Copy();
  }
  return right.Copy();
}

// Debug
auto TimestampType::ToString(const Value &val) const -> std::string {
  if (val.IsNull()) {
    return "timestamp_null";
  }

  uint64_t timestamp = val.value_.timestamp_;
  const auto micro = static_cast<uint32_t>(timestamp % 1000000);
  timestamp /= 1000000;
  auto second = static_cast<uint32_t>(timestamp % 100000);
  const auto sec = static_cast<uint16_t>(second % 60);
  second /= 60;
  const auto min = static_cast<uint16_t>(second % 60);
  second /= 60;
  const auto hour = static_cast<uint16_t>(second % 24);
  timestamp /= 100000;
  const auto year = static_cast<uint16_t>(timestamp % 10000);
  timestamp /= 10000;
  auto tz = static_cast<int>(timestamp % 27);
  tz -= 12;
  timestamp /= 27;
  const auto day = static_cast<uint16_t>(timestamp % 32);
  timestamp /= 32;
  const auto month = static_cast<uint16_t>(timestamp);

  constexpr size_t date_str_len = 30;
  constexpr size_t zone_len = 11;
  char str[date_str_len];
  char zone[zone_len];
  snprintf(str, date_str_len, "%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, min, sec, micro);  // NOLINT
  if (tz >= 0) {
    str[26] = '+';
  } else {
    str[26] = '-';
  }
  if (tz < 0) {
    tz = -tz;
  }
  snprintf(zone, zone_len, "%02d", tz);  // NOLINT
  str[27] = 0;
  return std::string(std::string(str) + std::string(zone));
}

void TimestampType::SerializeTo(const Value &val, char *storage) const {
  *reinterpret_cast<uint64_t *>(storage) = val.value_.timestamp_;
}

// Deserialize a value of the given type from the given storage space.
auto TimestampType::DeserializeFrom(const char *storage) const -> Value {
  uint64_t val = *reinterpret_cast<const uint64_t *>(storage);
  return {type_id_, val};
}

// Create a copy of this value
auto TimestampType::Copy(const Value &val) const -> Value { return {val}; }

auto TimestampType::CastAs(const Value &val, const TypeId type_id) const -> Value {
  switch (type_id) {  // NOLINT
    case TIMESTAMP:
      return Copy(val);
    case VARCHAR:
      if (val.IsNull()) {
        return ValueFactory::GetVarcharValue(nullptr, false);
      }
      return ValueFactory::GetVarcharValue(val.ToString());
    default:
      break;
  }
  throw Exception("TIMESTAMP is not coercible to " + GetInstance(type_id)->ToString(val));
}

}  // namespace bustub
