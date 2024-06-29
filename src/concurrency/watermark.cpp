#include "concurrency/watermark.h"

#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(const timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  if (const auto itr = current_reads_.find(read_ts); itr != current_reads_.end()) {
    itr->second++;
  } else {
    current_reads_[read_ts] = 1;
  }
}

auto Watermark::RemoveTxn(const timestamp_t read_ts) -> void {
  // Remove the read timestamp from the current reads.
  if (const auto itr = current_reads_.find(read_ts); itr != current_reads_.end()) {
    itr->second--;

    if (itr->second == 0) {
      current_reads_.erase(itr);

      // Update watermark.
      if (current_reads_.empty()) {
        watermark_ = commit_ts_;
      } else {
        watermark_ = current_reads_.begin()->first;
      }
    }
  }
}

}  // namespace bustub
