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
  explicit Watermark(const timestamp_t latest_commit_ts)
      : latest_commit_ts_(latest_commit_ts), current_watermark_(latest_commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(const timestamp_t commit_ts) {
    BUSTUB_ENSURE(commit_ts > latest_commit_ts_, "Commit timestamp must greater than latest commit timestamp.")
    latest_commit_ts_ = commit_ts;
  }

  [[nodiscard]] auto GetWatermark() const -> timestamp_t {
    if (active_read_timestamps_.empty()) {
      return latest_commit_ts_;
    }
    return current_watermark_;
  }

  [[nodiscard]] auto GetLatestCommitTimestamp() const -> timestamp_t { return latest_commit_ts_; }

 private:
  timestamp_t latest_commit_ts_;
  timestamp_t current_watermark_;
  std::map<timestamp_t, int> active_read_timestamps_;
};

};  // namespace bustub
