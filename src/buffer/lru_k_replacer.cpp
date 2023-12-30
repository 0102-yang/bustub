//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(const size_t num_frames, const size_t k) : max_replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lock(latch_);

  if (evictable_frame_size_ == 0) {
    return false;
  }

  const auto min = std::min_element(frames_.begin(), frames_.end());

  *frame_id = min->GetFrameId();
  frames_.erase(min);
  replacer_size_--;
  evictable_frame_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(const frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard lock(latch_);

  BUSTUB_ASSERT(replacer_size_ < max_replacer_size_, "LRU-K replacer is full");

  if (ContainsFrame(frame_id)) {
    const auto itr = FindFrame(frame_id);
    itr->RecordAccess();
  } else {
    frames_.emplace_back(frame_id, k_);
    replacer_size_++;
  }
}

void LRUKReplacer::SetEvictable(const frame_id_t frame_id, const bool set_evictable) {
  std::lock_guard lock(latch_);

  BUSTUB_ASSERT(ContainsFrame(frame_id), "Invalid frame id");

  if (const auto itr = FindFrame(frame_id); itr->GetEvictFlag() ^ set_evictable) {
    itr->SetEvictFlag(set_evictable);
    evictable_frame_size_ += set_evictable ? 1 : -1;
  }
}

void LRUKReplacer::Remove(const frame_id_t frame_id) {
  std::lock_guard lock(latch_);

  if (!ContainsFrame(frame_id)) {
    return;
  }

  const auto itr = FindFrame(frame_id);
  BUSTUB_ASSERT(itr->GetEvictFlag(), "Frame must be evictable");

  replacer_size_--;
  evictable_frame_size_--;

  frames_.erase(itr);
}

auto LRUKReplacer::ContainsFrame(const frame_id_t frame_id) -> bool {
  return frames_.end() != std::find_if(frames_.begin(), frames_.end(),
                                       [frame_id](const Frame &frame) { return frame.GetFrameId() == frame_id; });
}

auto LRUKReplacer::FindFrame(const frame_id_t frame_id) -> std::list<Frame>::iterator {
  return std::find_if(frames_.begin(), frames_.end(),
                      [frame_id](const Frame &frame) { return frame.GetFrameId() == frame_id; });
}

/************************************
 * Frame
 **************************************/
LRUKReplacer::Frame::Frame(const frame_id_t frame_id, const size_t k) : frame_id_(frame_id), k_(k) { RecordAccess(); }

void LRUKReplacer::Frame::RecordAccess() {
  if (frame_timestamps_.size() == k_) {
    frame_timestamps_.pop_front();
  }
  frame_timestamps_.push_back(Now());
}

auto LRUKReplacer::Frame::GetKDistanceTimestamp() const -> int64_t {
  int64_t timestamp = Now() - GetEarliestTimeStamp();
  if (frame_timestamps_.size() != k_) {
    timestamp += INF;
  }
  return timestamp;
}

auto LRUKReplacer::Frame::operator<(const Frame &other_frame) const -> bool {
  const bool evictable = GetEvictFlag();
  const bool other_evictable = other_frame.GetEvictFlag();

  if (evictable && !other_evictable) {
    return true;
  }
  if (other_evictable && !evictable) {
    return false;
  }

  const auto k_distance_timestamp = GetKDistanceTimestamp();
  const auto other_k_distance_timestamp = other_frame.GetKDistanceTimestamp();
  if (k_distance_timestamp > INF && other_k_distance_timestamp > INF) {
    return GetEarliestTimeStamp() < other_frame.GetEarliestTimeStamp();
  }
  return k_distance_timestamp > other_k_distance_timestamp;
}

}  // namespace bustub
