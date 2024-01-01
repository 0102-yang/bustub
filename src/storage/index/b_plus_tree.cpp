#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // Check whether tree is empty.
  if (IsEmpty()) {
    return false;
  }

  // Get key exist leaf page.
  auto leaf_page = GetKeyShouldExistLeafPage(key);
  bool key_exist = false;
  int index = 0;
  const int size = leaf_page->GetSize();

  while (index < size) {
    if (auto key_i = leaf_page->KeyAt(index); comparator_(key, key_i) == 0) {
      key_exist = true;
      auto value_i = leaf_page->ValueAt(index);
      result->push_back(value_i);
    }
    index++;
  }

  return key_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  LeafPage *leaf_page;

  /** Check if need a new root page. */
  if (IsEmpty()) {
    // Need to allocate a new page as root page.
    auto new_page = NewTreePage(IndexPageType::LEAF_PAGE);
    leaf_page = reinterpret_cast<LeafPage *>(new_page);
    // Update root page id.
    root_page_id_ = leaf_page->GetPageId();
    UpdateRootPageId();
  } else {
    // Get key should exist leaf page.
    leaf_page = GetKeyShouldExistLeafPage(key);
  }

  /** Check if there is already a key. */
  if (leaf_page->ContainsKey(key, comparator_)) {
    return false;
  }

  /** Insertion */
  if (leaf_page->GetMaxSize() == leaf_page->GetSize()) {
    /** Need to split leaf page. */
    // Create new leaf tree page.
    auto new_raw_page = NewTreePage(IndexPageType::LEAF_PAGE);
    auto new_leaf_page = reinterpret_cast<LeafPage *>(new_raw_page);

    // Allocate a memory to store new key-value and
    // all the key-value in leaf_page.
    auto leaf_size = leaf_page->GetSize();
    std::vector<MappingType> key_value;
    key_value.reserve(leaf_size + 1);

    for (int i = 0; i < leaf_size; i++) {
      key_value.emplace_back(leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    key_value.emplace_back(key, value);
    sort(key_value.begin(), key_value.end(),
         [this](const auto &e1, const auto &e2) -> bool { return comparator_(e1.first, e2.first) <= 0; });

    // Copy KV[n/2] ... KV[n] to new leaf tree page.
    int leaf_page_new_size = key_value.size() / 2;
    int copy_from_start_index = leaf_page_new_size;
    int new_leaf_page_size = key_value.size() - leaf_page_new_size;

    for (int copy_to_start_index = 0; copy_to_start_index < new_leaf_page_size;
         copy_to_start_index++, ++copy_from_start_index) {
      new_leaf_page->SetKeyAt(copy_to_start_index, key_value[copy_from_start_index].first);
      new_leaf_page->SetValueAt(copy_to_start_index, key_value[copy_from_start_index].second);
    }

    // Change size of both leaf page.
    // Lazily delete half key-value pair of leaf tree page.
    leaf_page->SetSize(leaf_page_new_size);
    new_leaf_page->SetSize(new_leaf_page_size);

    // Set b+tree parent and child pointers.
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    new_leaf_page->SetParentPageId(leaf_page->GetParentPageId());
    leaf_page->SetNextPageId(new_leaf_page->GetPageId());

    // Insert minimum key-value of new leaf tree page into parent tree page.
    auto minimum_key = new_leaf_page->KeyAt(0);
    Insert(leaf_page, new_leaf_page, minimum_key);

    // Unpin pages.
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  } else {
    // Just insert key value into page.
    leaf_page->Insert(key, value, comparator_);
    // Unpin page.
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }

  /** Debug. */
  const auto root_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  ToString(root_tree_page, buffer_pool_manager_);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Insert(BPlusTreePage *tree_page, BPlusTreePage *new_tree_page, const KeyType &key) {
  /**
   * If @param tree_page is already a root page,
   * need create a new internal page as root page.
   * This new internal page is parent of both @param tree_page
   * and @param new_tree_page.
   */
  if (tree_page->IsRootPage()) {
    auto new_raw_page = NewTreePage(IndexPageType::INTERNAL_PAGE);
    auto new_root_internal_page = reinterpret_cast<InternalPage *>(new_raw_page);
    auto new_root_page_id = new_root_internal_page->GetPageId();

    // Insert key and set pointers.
    new_root_internal_page->SetKeyAt(1, key);
    new_root_internal_page->SetValueAt(0, tree_page->GetPageId());
    new_root_internal_page->SetValueAt(1, new_tree_page->GetPageId());

    // Set b+tree pointers.
    tree_page->SetParentPageId(new_root_page_id);
    new_tree_page->SetParentPageId(new_root_page_id);

    // Set size of new root internal page.
    new_root_internal_page->SetSize(2);

    // Change root page.
    root_page_id_ = new_root_page_id;
    UpdateRootPageId();

    // Unpin page
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
  } else {
    auto parent_raw_page = buffer_pool_manager_->FetchPage(tree_page->GetParentPageId());
    auto parent_internal_page = reinterpret_cast<InternalPage *>(parent_raw_page);

    /**
     * If parent internal page is already full,
     * need to split parent internal page.
     */
    if (parent_internal_page->GetSize() == parent_internal_page->GetMaxSize()) {
      // Split parent internal page.
      auto new_raw_page = NewTreePage(IndexPageType::INTERNAL_PAGE);
      auto new_parent_internal_page = reinterpret_cast<InternalPage *>(new_raw_page);

      // Allocate a memory to store new key-value and
      // all the key-value in parent_internal_page.
      auto parent_internal_size = parent_internal_page->GetSize();
      std::vector<std::pair<KeyType, page_id_t>> key_pageid;
      key_pageid.reserve(parent_internal_size + 1);

      for (int i = 0; i < parent_internal_size; i++) {
        key_pageid.emplace_back(parent_internal_page->KeyAt(i), parent_internal_page->ValueAt(i));
      }
      key_pageid.emplace_back(key, new_tree_page->GetPageId());
      sort(key_pageid.begin(), key_pageid.end(),
           [this](const auto &e1, const auto &e2) -> bool { return comparator_(e1.first, e2.first) <= 0; });

      int parent_internal_page_new_size = static_cast<int>(key_pageid.size()) / 2;
      int copy_from_start_index = parent_internal_page_new_size;
      int new_parent_internal_page_size = static_cast<int>(key_pageid.size()) - parent_internal_page_new_size;
      KeyType insert_grandparent_key = key_pageid[copy_from_start_index].first;

      // Set key and pointers in new internal page.
      new_parent_internal_page->SetValueAt(0, key_pageid[copy_from_start_index++].second);
      for (int copy_to_start_index = 1; copy_to_start_index < new_parent_internal_page_size;
           copy_to_start_index++, ++copy_from_start_index) {
        new_parent_internal_page->SetKeyAt(copy_to_start_index, key_pageid[copy_from_start_index].first);
        new_parent_internal_page->SetValueAt(copy_to_start_index, key_pageid[copy_from_start_index].second);
      }

      // Set new size for both parent internal page
      // and new internal page.
      parent_internal_page->SetSize(parent_internal_page_new_size);
      new_parent_internal_page->SetSize(new_parent_internal_page_size);

      // Set b+tree parent and child pointers.
      new_parent_internal_page->SetParentPageId(parent_internal_page->GetParentPageId());

      // Insert special key into grandparent page.
      Insert(parent_internal_page, new_parent_internal_page, insert_grandparent_key);

      // Unpin pages.
      buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_parent_internal_page->GetPageId(), true);
    } else {
      // Just insert key and page id into parent internal
      // page.
      parent_internal_page->Insert(key, new_tree_page->GetPageId(), comparator_);
      buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetKeyShouldExistLeafPage(const KeyType &key) const -> LeafPage * {
  auto root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto tree_page = reinterpret_cast<BPlusTreePage *>(root_page);

  // Get specified leaf page.
  while (!tree_page->IsLeafPage()) {
    auto internal_page = reinterpret_cast<InternalPage *>(tree_page);
    int index = 1;
    int size = internal_page->GetSize();

    // Let index be the minimum value that satisfies key<=Ki.
    while (index < size) {
      auto key_i = internal_page->KeyAt(index);
      if (comparator_(key, key_i) < 0) {
        break;
      }
      index++;
    }

    // Get next page.
    int next_page_id = internal_page->ValueAt(index - 1);
    auto current_page_id = internal_page->GetPageId();
    auto next_page = buffer_pool_manager_->FetchPage(next_page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(next_page);
    // Unpin current page.
    buffer_pool_manager_->UnpinPage(current_page_id, false);
  }

  return reinterpret_cast<LeafPage *>(tree_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewTreePage(IndexPageType index_page_type) -> BPlusTreePage * {
  page_id_t new_page_id;
  buffer_pool_manager_->NewPage(&new_page_id);
  auto new_page = buffer_pool_manager_->FetchPage(new_page_id);

  if (index_page_type == IndexPageType::LEAF_PAGE) {
    auto leaf_page = reinterpret_cast<LeafPage *>(new_page);
    leaf_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(new_page);
    internal_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  }

  return reinterpret_cast<BPlusTreePage *>(new_page);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
