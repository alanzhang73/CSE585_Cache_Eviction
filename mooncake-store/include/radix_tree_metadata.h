#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mooncake {

// Tracks prefix/suffix structural dependencies between object keys. The tree
// only constrains which object keys are structurally safe to remove; the
// eviction policy still ranks among the resulting leaf candidates.
class RadixTreeMetadataIndex {
   public:
    void RegisterObject(const std::string& key,
                        const std::vector<std::string>& path_segments);
    void UnregisterObject(const std::string& key);
    bool IsLeafObject(const std::string& key) const;
    bool HasObject(const std::string& key) const;
    void Clear();

   private:
    struct Node {
        std::unordered_map<std::string, size_t> children;
        std::unordered_set<std::string> object_keys;
        size_t subtree_object_count{0};
    };

    size_t EnsureChild(size_t parent_index, const std::string& segment);
    void PruneIfEmpty(const std::vector<size_t>& path_indices,
                      const std::vector<std::string>& path_segments);

    mutable std::mutex mutex_;
    std::vector<Node> nodes_{1};
    std::unordered_map<std::string, std::vector<std::string>> key_to_path_;
};

}  // namespace mooncake
