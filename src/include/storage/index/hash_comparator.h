//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_comparator.h
//
// Identification: src/include/storage/index/hash_comparator.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/util/hash_util.h"

namespace bustub {

/**
 * HashComparator is for comparing hash_t, it returns:
 *  < 0 if lhs < rhs
 *  > 0 if lhs > rhs
 *  = 0 if lhs = rhs
 */
class HashComparator {
 public:
  auto operator()(const hash_t lhs, const hash_t rhs) -> int {
    if (lhs < rhs) {
      return -1;
    }
    return lhs > rhs ? 1 : 0;
  }
};

}  // namespace bustub
