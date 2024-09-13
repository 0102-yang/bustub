#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_manager, const TableInfo *table_info,
               TableHeap *table_heap);

void CheckWriteWriteConflict(Transaction *txn, const TableHeap *table_heap, const std::vector<RID> &rids);

void AppendAndLinkUndoLog(TransactionManager *txn_manager, Transaction *txn, table_oid_t table_oid, const RID &rid,
                          UndoLog log);

auto GetUndoLog(Transaction *txn, const RID &rid) -> std::optional<std::pair<UndoLog, size_t>>;

void TryUpdateUndoLog(Transaction *txn, const RID &rid, const Schema &schema,
                      const std::unordered_map<size_t, Value> &updated_row_old_values);

auto GetUndoLogSchema(const Schema &base_schema, const std::vector<bool> &modified_fields) -> Schema;

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
