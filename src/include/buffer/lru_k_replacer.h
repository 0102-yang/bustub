//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <chrono>  // NOLINT
#include <exception>
#include <list>
#include <mutex>  // NOLINT

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +INF as its backward k-distance. When multiple frames have +INF backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief A new LRUKReplacer.
   * @param num_frames The maximum number of frames the LRUReplacer will be required to store.
   * @param k The k-distance.
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +INF as its backward k-distance.
   * If multiple frames have INF backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than max_replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  [[nodiscard]] auto Size() const -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  struct Frame {
    Frame(frame_id_t frame_id, size_t k);

    [[nodiscard]] auto GetEvictFlag() const -> bool { return evict_flag_; }

    void SetEvictFlag(const bool evict_flag) { evict_flag_ = evict_flag; }

    [[nodiscard]] auto GetFrameId() const -> frame_id_t { return frame_id_; }

    void RecordAccess();

    [[nodiscard]] auto GetEarliestTimeStamp() const -> int64_t { return frame_timestamps_.front(); }

    [[nodiscard]] auto GetKDistanceTimestamp() const -> int64_t;

    /**
     * @brief Operator < means the frame is more likely to be evicted.
     */
    auto operator<(const Frame &other_frame) const -> bool;

    /**
     * @brief Get current timestamp with nanoseconds.
     */
    static auto Now() -> int64_t {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
    }

   private:
    const frame_id_t frame_id_;
    const size_t k_;
    bool evict_flag_ = false;
    std::list<int64_t> frame_timestamps_;
    static constexpr int64_t INF = 0x3f3f3f3f3f3f3f3f;
  };

  size_t replaceable_frame_size_ = 0;
  size_t current_size_ = 0;
  const size_t max_replacer_size_;
  const size_t k_;
  std::list<Frame> frames_;
  mutable std::mutex latch_;

  [[nodiscard]] auto ContainsFrame(frame_id_t frame_id) -> bool;

  [[nodiscard]] auto FindFrame(frame_id_t frame_id) -> std::list<Frame>::iterator;
};

}  // namespace bustub
