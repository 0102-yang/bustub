#include "concurrency/watermark.h"

#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(const timestamp_t read_ts) -> void {
  if (read_ts < latest_commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  if (const auto itr = active_read_timestamps_.find(read_ts); itr != active_read_timestamps_.end()) {
    itr->second++;
  } else {
    active_read_timestamps_[read_ts] = 1;
  }
}

auto Watermark::RemoveTxn(const timestamp_t read_ts) -> void {
  // Remove the read timestamp from the current reads.
  if (const auto itr = active_read_timestamps_.find(read_ts); itr != active_read_timestamps_.end()) {
    itr->second--;

    if (itr->second == 0) {
      active_read_timestamps_.erase(itr);

      // Update watermark.
      if (active_read_timestamps_.empty()) {
        current_watermark_ = latest_commit_ts_;
      } else {
        current_watermark_ = active_read_timestamps_.begin()->first;
      }
    }
  }
}

}  // namespace bustub
