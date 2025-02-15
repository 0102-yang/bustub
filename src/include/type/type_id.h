//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// type_id.h
//
// Identification: src/include/type/type_id.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace bustub {
// Every possible SQL type ID
enum TypeId : uint8_t { INVALID = 0, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, TIMESTAMP };
}  // namespace bustub
