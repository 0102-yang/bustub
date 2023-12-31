#include "primer/trie.h"

#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(const std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (!root_) {
    return nullptr;
  }

  auto current = root_;
  bool key_exist = true;
  for (const char &c : key) {
    if (current->children_.find(c) == current->children_.end()) {
      // c is not a child key of current root.
      key_exist = false;
      break;
    }

    // Iterate.
    const auto next = current->children_.at(c);
    current = next;
  }

  if (!key_exist) {
    // Key doesn't exist.
    return nullptr;
  }

  auto node_with_value = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(current);
  return node_with_value ? &*(node_with_value->value_) : nullptr;
}

template <class T>
[[nodiscard]] auto Trie::Put(const std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  /****************************************************
   * Iterate through the trie and copy the necessary nodes.
   ****************************************************/
  auto current_parent = root_;
  std::queue<std::pair<std::shared_ptr<const TrieNode>, const char>> search_path;
  search_path.emplace(root_, '\0');

  size_t i = 0;
  while (i < key.size()) {
    const char &c = key[i];
    std::shared_ptr<const TrieNode> current = nullptr;

    if (current_parent && current_parent->children_.find(c) != current_parent->children_.cend()) {
      current = current_parent->children_.at(c);
    }

    search_path.emplace(current, c);
    ++i;
    current_parent = current;
  }

  /*****************************************************
   * Copy.
   * 100% need to clone nodes.
   * Clone the necessary trie nodes from top to bottom.
   *****************************************************/
  std::shared_ptr<TrieNode> clone_aux;
  std::shared_ptr<const TrieNode> new_root;

  while (!search_path.empty()) {
    const auto &[original, c] = search_path.front();
    search_path.pop();

    std::shared_ptr<TrieNode> clone;
    if (search_path.empty()) {
      // Reach last node.
      std::shared_ptr v = std::make_shared<T>(std::move(value));
      clone = original ? std::make_shared<TrieNodeWithValue<T>>(original->children_, v)
                       : clone = std::make_shared<TrieNodeWithValue<T>>(v);
    } else {
      clone = original ? original->Clone() : std::make_shared<TrieNode>();
    }

    if (clone_aux) {
      clone_aux->children_[c] = clone;
      clone_aux = clone;
    } else {
      clone_aux = clone;
      new_root = clone_aux;
    }
  }

  return Trie(new_root);
}

auto Trie::Remove(const std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value anymore,
  // you should convert it to `TrieNode`. If a node doesn't have children anymore, you should remove it.
  if (!root_) {
    return {};
  }

  auto current = root_;
  std::stack<std::pair<std::shared_ptr<const TrieNode>, const char>> search_path;
  bool key_exist = true;

  for (const char &c : key) {
    if (current->children_.find(c) == current->children_.end()) {
      // c is not a child key of current root.
      key_exist = false;
      break;
    }

    // Iterate.
    search_path.emplace(current, c);
    const auto next = current->children_.at(c);
    current = next;
  }

  if (!key_exist || !current->is_value_node_) {
    return Trie(root_);
  }

  // Recursively remove nodes if necessary.
  if (current->children_.empty()) {
    current = nullptr;
  } else {
    current = std::make_shared<TrieNode>(current->children_);
  }
  while (!search_path.empty()) {
    const auto &[parent, c] = search_path.top();
    search_path.pop();

    const std::shared_ptr new_parent = parent->Clone();
    if (current) {
      new_parent->children_[c] = current;
      current = new_parent;
    } else {
      new_parent->children_.erase(c);
      if (!new_parent->children_.empty() || new_parent->is_value_node_) {
        current = new_parent;
      }
    }
  }

  return Trie(current);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
