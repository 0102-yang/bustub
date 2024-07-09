#pragma once

#include <map>

#include "storage/table/tuple.h"

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
 public:
  explicit Watermark(const timestamp_t latest_commit_timestamp)
      : latest_commit_timestamp_(latest_commit_timestamp), watermark_(latest_commit_timestamp) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(const timestamp_t commit_ts) {
    BUSTUB_ENSURE(commit_ts > latest_commit_timestamp_, "Commit timestamp must greater than latest commit timestamp.")
    latest_commit_timestamp_ = commit_ts;
  }

  [[nodiscard]] auto GetWatermark() const -> timestamp_t {
    if (current_reads_.empty()) {
      return latest_commit_timestamp_;
    }
    return watermark_;
  }

  [[nodiscard]] auto GetLatestCommitTimestamp() const -> timestamp_t { return latest_commit_timestamp_; }

 private:
  timestamp_t latest_commit_timestamp_;
  timestamp_t watermark_;
  std::map<timestamp_t, int> current_reads_;
};

};  // namespace bustub
