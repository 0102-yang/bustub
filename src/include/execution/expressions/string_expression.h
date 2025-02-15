//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// string_expression.h
//
// Identification: src/include/expression/string_expression.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/exception.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

enum class StringExpressionType : uint8_t { Lower, Upper };

/**
 * StringExpression represents two expressions being computed.
 */
class StringExpression final : public AbstractExpression {
 public:
  StringExpression(AbstractExpressionRef arg, const StringExpressionType expr_type)
      : AbstractExpression({std::move(arg)}, VARCHAR), expr_type_{expr_type} {
    if (GetChildAt(0)->GetReturnType() != VARCHAR) {
      throw NotImplementedException("expect the first arg to be varchar");
    }
  }

  [[nodiscard]] auto Compute(const std::string &val) const -> std::string {
    // (student): implement upper / lower.
    std::string res;
    switch (expr_type_) {
      case bustub::StringExpressionType::Lower:
        res.reserve(val.length());
        std::transform(val.begin(), val.end(), std::back_inserter(res), tolower);
        break;
      case bustub::StringExpressionType::Upper:
        res.reserve(val.length());
        std::transform(val.begin(), val.end(), std::back_inserter(res), toupper);
        break;
      default:
        break;
    }
    return res;
  }

  auto Evaluate(const Tuple *tuple, const Schema &schema) const -> Value override {
    const Value val = GetChildAt(0)->Evaluate(tuple, schema);
    const auto str = val.GetAs<char *>();
    return ValueFactory::GetVarcharValue(Compute(str));
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple,
                    const Schema &right_schema) const -> Value override {
    const Value val = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    const auto str = val.GetAs<char *>();
    return ValueFactory::GetVarcharValue(Compute(str));
  }

  /** @return the string representation of the expression node and its children */
  [[nodiscard]] auto ToString() const -> std::string override {
    return fmt::format("{}({})", expr_type_, *GetChildAt(0));
  }

  BUSTUB_EXPR_CLONE_WITH_CHILDREN(StringExpression)

  StringExpressionType expr_type_;
};
}  // namespace bustub

template <>
struct fmt::formatter<bustub::StringExpressionType> : formatter<string_view> {
  template <typename FormatContext>
  auto format(bustub::StringExpressionType c, FormatContext &ctx) const {
    string_view name;
    switch (c) {
      case bustub::StringExpressionType::Upper:
        name = "upper";
        break;
      case bustub::StringExpressionType::Lower:
        name = "lower";
        break;
      default:
        name = "Unknown";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
