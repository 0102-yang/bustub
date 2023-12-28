//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <stack>
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(const char key_char) : key_char_(key_char) {}

  TrieNode(const TrieNode &) = delete;

  auto operator=(const TrieNode &) -> TrieNode & = delete;

  auto operator=(TrieNode &&) -> TrieNode & = delete;

  /**
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    key_char_ = other_trie_node.key_char_;
    children_ = std::move(other_trie_node.children_);
    is_end_ = other_trie_node.is_end_;
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  ~TrieNode() = default;

  /**
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  auto HasChild(const char key_char) const -> bool { return children_.find(key_char) != children_.end(); }

  /**
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  auto HasChildren() const -> bool { return !children_.empty(); }

  /**
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  auto IsEndNode() const -> bool { return is_end_; }

  /**
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  auto GetKeyChar() const -> char { return key_char_; }

  /**
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key_char Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  auto InsertChildNode(const char key_char, std::unique_ptr<TrieNode> &&child) -> std::unique_ptr<TrieNode> * {
    if (children_.find(key_char) != children_.end() || key_char != child->key_char_) {
      return nullptr;
    }

    children_.emplace(key_char, std::move(child));
    return &children_[key_char];
  }

  /**
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key_char Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  auto GetChildNode(const char key_char) -> std::unique_ptr<TrieNode> * {
    if (children_.find(key_char) == children_.end()) {
      return nullptr;
    }
    return &children_[key_char];
  }

  /**
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(const char key_char) {
    if (children_.find(key_char) == children_.end()) {
      return;
    }
    children_.erase(key_char);
  }

  /**
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(const bool is_end) { is_end_ = is_end; }

 protected:
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trie_node TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value The value
   */
  TrieNodeWithValue(TrieNode &&trie_node, T value) : TrieNode(std::move(trie_node)), value_{value} {}

  /**
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(const char key_char, T value) : TrieNode(key_char), value_{value} {}

  TrieNodeWithValue(const TrieNodeWithValue &) = delete;

  TrieNodeWithValue(TrieNodeWithValue &&) = delete;

  auto operator=(const TrieNodeWithValue &) = delete;

  auto operator=(TrieNodeWithValue &&) = delete;

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  auto GetValue() const -> T { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() { root_ = std::make_unique<TrieNode>('\0'); }

  /**
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  auto Insert(const std::string &key, T value) -> bool {
    const std::unique_ptr<TrieNode> *current_parent = &root_;
    latch_.WLock();

    for (size_t i = 0; i < key.size(); i++) {
      const char key_char = key[i];
      auto current_node = (*current_parent)->InsertChildNode(key_char, std::make_unique<TrieNode>(key_char));
      current_node = current_node ? current_node : (*current_parent)->GetChildNode(key_char);

      // Reach the terminal node
      if (i == key.size() - 1) {
        if ((*current_node)->IsEndNode()) {
          // It is already a TrieNodeWithValue
          break;
        }

        // Turn terminal node from type `TrieNode` to `TrieNodeWithValue`
        current_node->reset(new TrieNodeWithValue<T>(std::move(**current_node), value));
        (*current_node)->SetEndNode(true);
        latch_.WUnlock();
        return true;
      }

      current_parent = current_node;
    }

    latch_.WUnlock();
    return false;
  }

  /**
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  auto Remove(const std::string &key) -> bool {
    std::stack<TrieNode *> recursive_search_stack;
    std::unordered_map<TrieNode *, TrieNode *> child_parent_node_kv;
    bool is_found = false;

    latch_.WLock();
    const std::unique_ptr<TrieNode> *current_parent = &root_;

    for (size_t i = 0; i < key.size(); i++) {
      const char key_char = key[i];

      // Key is not found
      if (!(*current_parent)->HasChild(key_char)) {
        break;
      }

      TrieNode *root = current_parent->get();

      // Reach the terminal node
      if (i == key.size() - 1) {
        const auto current_child = root->GetChildNode(key_char);

        // It is not a TrieNodeWithValue
        if (!(*current_child)->IsEndNode()) {
          break;
        }

        // Reset is_end_ flag of TrieNodeWithValue to false
        (*current_child)->SetEndNode(false);
        is_found = true;
      }

      // Update search stack
      TrieNode *child = root->GetChildNode(key_char)->get();
      child_parent_node_kv.emplace(child, root);
      recursive_search_stack.push(child);

      // Iterate
      current_parent = root->GetChildNode(key_char);
    }

    if (!is_found) {
      latch_.WUnlock();
      return false;
    }

    // If find node to remove, recursively remove node
    while (!recursive_search_stack.empty()) {
      TrieNode *node_ptr = recursive_search_stack.top();
      recursive_search_stack.pop();

      if (node_ptr->IsEndNode() || node_ptr->HasChildren()) {
        break;
      }

      // Remove current `TrieNode` node
      TrieNode *parent_node_ptr = child_parent_node_kv[node_ptr];
      parent_node_ptr->RemoveChildNode(node_ptr->GetKeyChar());
    }

    latch_.WUnlock();
    return true;
  }

  /**
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the cast result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  auto GetValue(const std::string &key, bool *success) -> T {
    latch_.RLock();
    const std::unique_ptr<TrieNode> *current_parent = &root_;

    for (size_t i = 0; i < key.size(); i++) {
      const char key_char = key[i];

      // Iterate root first
      if (!(*current_parent)->HasChild(key_char)) {
        break;
      }
      current_parent = (*current_parent)->GetChildNode(key_char);

      // If reach the terminal node
      if (i == key.size() - 1) {
        // Not a end node
        if (!(*current_parent)->IsEndNode()) {
          break;
        }

        // Find the value
        *success = true;
        auto node_with_value = static_cast<TrieNodeWithValue<T> *>(current_parent->get());
        T ret_value = node_with_value->GetValue();
        latch_.RUnlock();
        return ret_value;
      }
    }

    latch_.RUnlock();
    *success = false;
    return {};
  }
};
}  // namespace bustub