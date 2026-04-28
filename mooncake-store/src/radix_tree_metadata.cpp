#include "radix_tree_metadata.h"

#include <stdexcept>

namespace mooncake {

size_t RadixTreeMetadataIndex::EnsureChild(size_t parent_index,
                                           const std::string& segment) {
    auto it = nodes_[parent_index].children.find(segment);
    if (it != nodes_[parent_index].children.end()) {
        return it->second;
    }

    const size_t child_index = nodes_.size();
    nodes_.emplace_back();
    nodes_[parent_index].children.emplace(segment, child_index);
    return child_index;
}

void RadixTreeMetadataIndex::RegisterObject(
    const std::string& key, const std::vector<std::string>& path_segments) {
    if (path_segments.empty()) {
        throw std::invalid_argument("radix path segments must not be empty");
    }

    UnregisterObject(key);

    std::lock_guard<std::mutex> lock(mutex_);
    size_t node_index = 0;
    nodes_[node_index].subtree_object_count++;
    for (const auto& segment : path_segments) {
        node_index = EnsureChild(node_index, segment);
        nodes_[node_index].subtree_object_count++;
    }
    nodes_[node_index].object_keys.insert(key);
    key_to_path_[key] = path_segments;
}

void RadixTreeMetadataIndex::PruneIfEmpty(
    const std::vector<size_t>& path_indices,
    const std::vector<std::string>& path_segments) {
    for (size_t i = path_indices.size(); i > 1; --i) {
        const size_t node_index = path_indices[i - 1];
        Node& node = nodes_[node_index];
        if (node.subtree_object_count != 0 || !node.children.empty() ||
            !node.object_keys.empty()) {
            break;
        }

        Node& parent = nodes_[path_indices[i - 2]];
        parent.children.erase(path_segments[i - 2]);
    }
}

void RadixTreeMetadataIndex::UnregisterObject(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto path_it = key_to_path_.find(key);
    if (path_it == key_to_path_.end()) {
        return;
    }

    const auto& path_segments = path_it->second;
    std::vector<size_t> path_indices;
    path_indices.reserve(path_segments.size() + 1);
    size_t node_index = 0;
    path_indices.push_back(node_index);
    for (const auto& segment : path_segments) {
        auto child_it = nodes_[node_index].children.find(segment);
        if (child_it == nodes_[node_index].children.end()) {
            key_to_path_.erase(path_it);
            return;
        }
        node_index = child_it->second;
        path_indices.push_back(node_index);
    }

    nodes_[node_index].object_keys.erase(key);
    for (size_t index : path_indices) {
        if (nodes_[index].subtree_object_count > 0) {
            nodes_[index].subtree_object_count--;
        }
    }

    PruneIfEmpty(path_indices, path_segments);
    key_to_path_.erase(path_it);
}

bool RadixTreeMetadataIndex::IsLeafObject(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto path_it = key_to_path_.find(key);
    if (path_it == key_to_path_.end()) {
        return true;
    }

    size_t node_index = 0;
    for (const auto& segment : path_it->second) {
        auto child_it = nodes_[node_index].children.find(segment);
        if (child_it == nodes_[node_index].children.end()) {
            return true;
        }
        node_index = child_it->second;
    }

    const auto& node = nodes_[node_index];
    return node.subtree_object_count == node.object_keys.size();
}

bool RadixTreeMetadataIndex::HasObject(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return key_to_path_.contains(key);
}

void RadixTreeMetadataIndex::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    nodes_.clear();
    nodes_.emplace_back();
    key_to_path_.clear();
}

}  // namespace mooncake
