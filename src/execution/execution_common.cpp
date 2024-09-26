#include <iosfwd>
#include <sstream>

#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

#include "common/logger.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

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

    // Get modified tuple schema and modify reconstructed tuple.
    Schema partial_schema = GetUndoLogSchema(*schema, modified_fields_);
    for (uint32_t i = 0, modified_idx = 0; i < modified_fields_.size(); i++) {
      if (modified_fields_[i]) {
        reconstructed_tuple.SetValue(schema, i, tuple_.GetValue(&partial_schema, modified_idx++));
      }
    }
  }

  return is_deleted ? std::nullopt : std::make_optional(reconstructed_tuple);
}

auto IsDanglingUndoLink(const UndoLink &link, TransactionManager *txn_manager) -> bool {
  std::shared_lock lock(txn_manager->txn_map_mutex_);
  return txn_manager->txn_map_.find(link.prev_txn_) == txn_manager->txn_map_.end();
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_manager, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "Debug hook: {}", info);

  const auto format_timestamp = [](const timestamp_t &ts) -> std::string {
    return ts >= TXN_START_ID ? "temp_ts" + std::to_string(ts - TXN_START_ID) : std::to_string(ts);
  };

  auto itr = table_heap->MakeIterator();
  const auto &schema = table_info->schema_;
  const auto watermark = txn_manager->GetWatermark();
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
    auto optional_version_link = txn_manager->GetVersionLink(rid);
    if (!optional_version_link.has_value()) {
      continue;
    }

    auto undo_link = optional_version_link->prev_;
    uint count = 0;
    std::vector<UndoLog> undo_logs;
    while (undo_link.IsValid()) {
      if (IsDanglingUndoLink(undo_link, txn_manager)) {
        break;
      }

      const auto undo_log = txn_manager->GetUndoLog(undo_link);
      undo_logs.push_back(undo_log);

      // Get versioned tuple from undo logs.
      if (const auto versioned_tuple = ReconstructTuple(&schema, tuple, meta, undo_logs); versioned_tuple.has_value()) {
        if (undo_log.ts_ < watermark) {
          debug_output << fmt::format("  {}: tuple={} ts={} <GCed>\n", count, versioned_tuple->ToString(&schema),
                                      format_timestamp(undo_log.ts_));
        } else {
          debug_output << fmt::format("  {}: tuple={} ts={}\n", count, versioned_tuple->ToString(&schema),
                                      format_timestamp(undo_log.ts_));
        }
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

void CheckWriteWriteConflict(Transaction *txn, const TableHeap *table_heap, const std::vector<RID> &rids) {
  for (const auto &rid : rids) {
    if (const auto [ts_, is_deleted_] = table_heap->GetTupleMeta(rid);
        ts_ > txn->GetReadTs() && ts_ != txn->GetTransactionId()) {
      txn->SetTainted();
      throw ExecutionException("Write-Write conflict detected.");
    }
  }
}

void AppendAndLinkUndoLog(TransactionManager *txn_manager, Transaction *txn, const table_oid_t table_oid,
                          const RID &rid, UndoLog log) {
  txn->AppendWriteSet(table_oid, rid);
  log.prev_version_ = txn_manager->GetUndoLink(rid).value_or(UndoLink{});
  UndoLink new_link = txn->AppendUndoLog(log);
  txn_manager->UpdateUndoLink(rid, new_link, nullptr);
}

auto GetUndoLog(Transaction *txn, const RID &rid) -> std::optional<std::pair<UndoLog, size_t>> {
  for (size_t log_idx = 0; log_idx < txn->GetUndoLogNum(); log_idx++) {
    if (auto log = txn->GetUndoLog(log_idx); log.tuple_.GetRid() == rid) {
      return std::make_pair(log, log_idx);
    }
  }
  return std::nullopt;
}

void TryUpdateUndoLog(Transaction *txn, const RID &rid, const Schema &schema,
                      const std::unordered_map<size_t, Value> &updated_row_old_values) {
  const auto &undo_log_entry = GetUndoLog(txn, rid);
  if (!undo_log_entry.has_value()) {
    return;
  }

  const auto &base_modified_fields = undo_log_entry->first.modified_fields_;
  std::vector<Value> new_undo_log_values;
  std::vector<bool> new_undo_log_modified_fields;
  new_undo_log_values.reserve(base_modified_fields.size());
  new_undo_log_modified_fields.reserve(base_modified_fields.size());
  const auto base_schema = GetUndoLogSchema(schema, base_modified_fields);

  for (size_t field_idx = 0, base_value_idx = 0; field_idx < base_modified_fields.size(); field_idx++) {
    const bool base_field = base_modified_fields[field_idx];
    const bool new_field = updated_row_old_values.find(field_idx) != updated_row_old_values.end();
    new_undo_log_modified_fields.push_back(base_field || new_field);
    if (!base_field && !new_field) {
      continue;
    }

    if (base_field) {
      new_undo_log_values.push_back(
          undo_log_entry->first.tuple_.GetValue(&base_schema, static_cast<uint32_t>(base_value_idx++)));
    } else {
      new_undo_log_values.push_back(updated_row_old_values.at(field_idx));
    }
  }

  // Generate new undo log and update it.
  const Schema new_undo_log_schema = GetUndoLogSchema(schema, new_undo_log_modified_fields);
  Tuple new_undo_log_tuple{new_undo_log_values, &new_undo_log_schema};
  new_undo_log_tuple.SetRid(rid);
  const UndoLog new_undo_log{undo_log_entry->first.is_deleted_, new_undo_log_modified_fields, new_undo_log_tuple,
                             undo_log_entry->first.ts_, undo_log_entry->first.prev_version_};
  txn->ModifyUndoLog(undo_log_entry->second, new_undo_log);
}

auto GetUndoLogSchema(const Schema &base_schema, const std::vector<bool> &modified_fields) -> Schema {
  std::vector<uint32_t> attrs;
  for (uint32_t column_idx = 0U; column_idx < modified_fields.size(); column_idx++) {
    if (modified_fields[column_idx]) {
      attrs.emplace_back(column_idx);
    }
  }
  return Schema::CopySchema(&base_schema, attrs);
}

}  // namespace bustub
