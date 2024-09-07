#include <iosfwd>
#include <sstream>

#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  const auto get_undo_log_schema = [](const UndoLog &undo_log, const Schema &schema) -> Schema {
    std::vector<uint32_t> attrs;
    for (uint32_t column_idx = 0U; column_idx < undo_log.modified_fields_.size(); column_idx++) {
      if (undo_log.modified_fields_[column_idx]) {
        attrs.emplace_back(column_idx);
      }
    }
    return Schema::CopySchema(&schema, attrs);
  };

  bool is_deleted = base_meta.is_deleted_;
  Tuple reconstructed_tuple(base_tuple);
  for (const auto &undo_log : undo_logs) {
    is_deleted = undo_log.is_deleted_;
    if (undo_log.is_deleted_) {
      continue;
    }

    // Get modified tuple schema.
    Schema partial_schema = get_undo_log_schema(undo_log, *schema);

    // Modify reconstructed tuple.
    for (uint32_t i = 0, modified_idx = 0; i < undo_log.modified_fields_.size(); i++) {
      if (undo_log.modified_fields_[i]) {
        reconstructed_tuple.SetValue(schema, i, undo_log.tuple_.GetValue(&partial_schema, modified_idx++));
      }
    }
  }

  return is_deleted ? std::nullopt : std::make_optional(reconstructed_tuple);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "Debug hook: {}", info);

  const auto format_timestamp = [](const timestamp_t &ts) -> std::string {
    return ts >= TXN_START_ID ? "txn" + std::to_string(ts - TXN_START_ID) : std::to_string(ts);
  };

  auto itr = table_heap->MakeIterator();
  const auto &schema = table_info->schema_;
  std::ostringstream debug_output;
  while (!itr.IsEnd()) {
    // Get tuple.
    const auto &rid = itr.GetRID();
    const auto &[meta, tuple] = itr.GetTuple();
    ++itr;

    if (meta.is_deleted_) {
      debug_output << fmt::format("RID={}/{} ts={} <del> tuple={}\n", rid.GetPageId(), rid.GetSlotNum(),
                                  format_timestamp(meta.ts_), tuple.ToString(&schema));
    } else {
      debug_output << fmt::format("RID={}/{} ts={} tuple={}\n", rid.GetPageId(), rid.GetSlotNum(),
                                  format_timestamp(meta.ts_), tuple.ToString(&schema));
    }

    // Get undo link.
    auto optional_version_link = txn_mgr->GetVersionLink(rid);
    if (!optional_version_link.has_value()) {
      continue;
    }

    auto undo_link = optional_version_link->prev_;
    uint count = 0;
    std::vector<UndoLog> undo_logs;
    while (undo_link.IsValid()) {
      const auto undo_log = txn_mgr->GetUndoLog(undo_link);
      undo_logs.push_back(undo_log);

      // Get versioned tuple from undo logs.
      if (const auto versioned_tuple = ReconstructTuple(&schema, tuple, meta, undo_logs); versioned_tuple.has_value()) {
        debug_output << fmt::format("  {}: tuple={} ts={}\n", count, versioned_tuple->ToString(&schema),
                                    format_timestamp(undo_log.ts_));
      } else {
        debug_output << fmt::format("  {}: <del> ts={}\n", count, undo_log.ts_);
      }

      undo_link = undo_log.prev_version_;
      count++;
    }
  }

  fmt::print(stderr, "{}", debug_output.str());

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
