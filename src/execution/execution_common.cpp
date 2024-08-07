#include <algorithm>

#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  bool is_deleted = base_meta.is_deleted_;
  Tuple reconstructed_tuple(base_tuple);

  for (const auto &[is_deleted_, modified_fields_, tuple_, ts_, prev_version_] : undo_logs) {
    is_deleted = is_deleted_;
    if (is_deleted_) {
      continue;
    }

    // Get modified tuple schema.
    std::vector<uint32_t> attrs;
    for (uint32_t column_idx = 0U; column_idx < modified_fields_.size(); column_idx++) {
      if (modified_fields_[column_idx]) {
        attrs.emplace_back(column_idx);
      }
    }
    Schema partial_schema = Schema::CopySchema(schema, attrs);

    // Modify reconstructed tuple.
    for (uint32_t i = 0, modified_idx = 0; i < modified_fields_.size(); i++) {
      if (modified_fields_[i]) {
        reconstructed_tuple.SetValue(schema, i, tuple_.GetValue(&partial_schema, modified_idx++));
      }
    }
  }

  return is_deleted ? std::nullopt : std::make_optional(reconstructed_tuple);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
