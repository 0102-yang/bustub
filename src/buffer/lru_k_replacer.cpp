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

#include <pstl/glue_numeric_defs.h>

#include "common/exception.h"

namespace bustub {

/**************************
 * LRUKNode implementation.
 **************************/

LRUKNode::LRUKNode(const size_t k, const frame_id_t fid) : k_(k), fid_(fid) {}

auto LRUKNode::GetFid() const -> frame_id_t { return fid_; }

[[nodiscard]] auto LRUKNode::GetBackwardKDist() const -> size_t {
  BUSTUB_ASSERT(history_.size() == k_, "There must be k timestamps in history");
  return history_.back();
}

[[nodiscard]] auto LRUKNode::GetEarliestTimestamp() const -> size_t {
  BUSTUB_ASSERT(!history_.empty(), "There must be at least 1 timestamp in history");
  return history_.front();
}

[[nodiscard]] auto LRUKNode::HasInfBackwardKDist() const -> bool { return history_.size() < k_; }

[[nodiscard]] auto LRUKNode::IsEvictable() const -> bool { return is_evictable_; }

void LRUKNode::SetEvictable(bool evictable) { is_evictable_ = evictable; }

void LRUKNode::InsertHistoryTimestamp(const size_t current_timestamp) {
  if (history_.size() == k_) {
    history_.pop_back();
  }
  history_.push_front(current_timestamp);
}

/******************************
 * LRUKReplacer implementation.
 ******************************/

LRUKReplacer::LRUKReplacer(const size_t num_frames, const size_t k) : max_replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lock(latch_);

  if (Size() == 0) {
    // No frames can be evicted.
    return false;
  }

  /**************
   * Evict frame.
   **************/
  std::vector<const LRUKNode *> evictable_nodes;
  for (const auto &[_, node] : node_store_) {
    if (node.IsEvictable()) {
      evictable_nodes.push_back(&node);
    }
  }

  std::vector<const LRUKNode *> evictable_inf_nodes;
  std::copy_if(evictable_nodes.begin(), evictable_nodes.end(), std::back_inserter(evictable_inf_nodes),
               [](const LRUKNode *node) { return node->HasInfBackwardKDist(); });

  if (evictable_inf_nodes.empty()) {
    // All frames have non-INF k-distance, then evict a frame
    // whose backward k-distance is maximum of all frames in the replacer.
    const LRUKNode *min_node = *std::min_element(evictable_nodes.begin(), evictable_nodes.end(),
                                                 [](const LRUKNode *node1, const LRUKNode *node2) {
                                                   return node1->GetBackwardKDist() < node2->GetBackwardKDist();
                                                 });
    *frame_id = min_node->GetFid();
  } else {
    // While multiple frames have INF backward k-distance, then evict the frame with the earliest
    // timestamp overall.
    const LRUKNode *min_node = *std::min_element(evictable_inf_nodes.begin(), evictable_inf_nodes.end(),
                                                 [](const LRUKNode *node1, const LRUKNode *node2) {
                                                   return node1->GetEarliestTimestamp() < node2->GetEarliestTimestamp();
                                                 });
    *frame_id = min_node->GetFid();
  }

  node_store_.erase(*frame_id);
  replacer_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(const frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard lock(latch_);

  BUSTUB_ASSERT(replacer_size_ <= max_replacer_size_, "LRU-K replacer is full");

  if (node_store_.count(frame_id) == 0) {
    // Frame not exist in replacer.
    node_store_.emplace(frame_id, LRUKNode{k_, frame_id});
    replacer_size_++;
  }
  node_store_.at(frame_id).InsertHistoryTimestamp(GetCurrentTimestamp());
}

void LRUKReplacer::SetEvictable(const frame_id_t frame_id, const bool set_evictable) {
  std::lock_guard lock(latch_);

  BUSTUB_ASSERT(node_store_.count(frame_id) > 0, "Invalid frame id");

  node_store_.at(frame_id).SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(const frame_id_t frame_id) {
  std::lock_guard lock(latch_);

  if (node_store_.count(frame_id) == 0) {
    return;
  }

  // Remove.
  replacer_size_--;
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() const -> size_t {
  return std::count_if(node_store_.begin(), node_store_.end(), [](const auto &p) { return p.second.IsEvictable(); });
}

auto LRUKReplacer::GetCurrentTimestamp() -> size_t {
  const auto timestamp = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp).count();
}

}  // namespace bustub
