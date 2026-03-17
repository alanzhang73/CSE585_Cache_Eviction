#pragma once

#include <memory>
#include <random>
#include <string>
#include <unordered_map>

#include "types.h"

namespace mooncake {

/**
 * @brief Abstract interface for eviction strategy, responsible for choosing
 *        which kvcache object to be evicted before pool overflow.
 */
class EvictionStrategy : public std::enable_shared_from_this<EvictionStrategy> {
   public:
    virtual ~EvictionStrategy() = default;
    virtual ErrorCode AddKey(const std::string& key) = 0;
    virtual ErrorCode UpdateKey(const std::string& key) = 0;
    virtual ErrorCode RemoveKey(const std::string& key) {
        // Remove key from the list and map
        auto it = all_key_idx_map_.find(key);
        if (it != all_key_idx_map_.end()) {
            all_key_list_.erase(it->second);
            all_key_idx_map_.erase(it);
        }
        return ErrorCode::OK;
    }
    virtual std::string EvictKey(void) = 0;
    virtual size_t GetSize(void) { return all_key_list_.size(); }
    void CleanUp(void) {
        all_key_list_.clear();
        all_key_idx_map_.clear();
    }

   protected:
    std::list<std::string> all_key_list_;
    std::unordered_map<std::string, std::list<std::string>::iterator>
        all_key_idx_map_;
};

class LRUEvictionStrategy : public EvictionStrategy {
   public:
    virtual ErrorCode AddKey(const std::string& key) override {
        // Add key to the front of the list
        if (all_key_idx_map_.find(key) != all_key_idx_map_.end()) {
            all_key_list_.erase(all_key_idx_map_[key]);
            all_key_idx_map_.erase(key);
        }
        all_key_list_.push_front(key);
        all_key_idx_map_[key] = all_key_list_.begin();
        return ErrorCode::OK;
    }

    virtual ErrorCode UpdateKey(const std::string& key) override {
        // Move the key to the front of the list
        auto it = all_key_idx_map_.find(key);
        if (it != all_key_idx_map_.end()) {
            all_key_list_.erase(it->second);
            all_key_list_.push_front(key);
            all_key_idx_map_[key] = all_key_list_.begin();
        }
        return ErrorCode::OK;
    }

    virtual std::string EvictKey(void) override {
        // Evict the last key in the list
        if (all_key_list_.empty()) {
            return "";
        }
        std::string evicted_key = all_key_list_.back();
        all_key_list_.pop_back();
        all_key_idx_map_.erase(evicted_key);
        return evicted_key;
    }
};

class FIFOEvictionStrategy : public EvictionStrategy {
   public:
    virtual ErrorCode AddKey(const std::string& key) override {
        // Add key to the front of the list
        all_key_list_.push_front(key);
        return ErrorCode::OK;
    }
    virtual ErrorCode UpdateKey(const std::string& key) override {
        return ErrorCode::OK;
    }
    virtual std::string EvictKey(void) {
        if (all_key_list_.empty()) {
            return "";
        }
        std::string evicted_key = all_key_list_.back();
        all_key_list_.pop_back();
        return evicted_key;
    }
};

class ClockEvictionStrategy : public EvictionStrategy {
   public:
    ClockEvictionStrategy() {
        clock_hand_ = all_key_list_.end();
    }

    virtual ErrorCode AddKey(const std::string& key) override {
        // If key exists, just update its bit and return
        if (all_key_idx_map_.find(key) != all_key_idx_map_.end()) {
            return UpdateKey(key);
        }

        // Add to the list
        all_key_list_.push_back(key);
        all_key_idx_map_[key] = std::prev(all_key_list_.end());
        use_bits_[key] = true;

        // Initialize hand if this is the first element
        if (all_key_list_.size() == 1) {
            clock_hand_ = all_key_list_.begin();
        }
        return ErrorCode::OK;
    }

    virtual ErrorCode UpdateKey(const std::string& key) override {
        // In Clock, we don't move the node (saves CPU). 
        // We just set the bit to true.
        if (all_key_idx_map_.find(key) != all_key_idx_map_.end()) {
            use_bits_[key] = true;
        }
        return ErrorCode::OK;
    }

    virtual ErrorCode RemoveKey(const std::string& key) override {
        auto it = all_key_idx_map_.find(key);
        if (it != all_key_idx_map_.end()) {
            // If we are removing the node the hand is pointing to, 
            // move the hand forward first.
            if (clock_hand_ == it->second) {
                AdvanceHand();
            }
            
            // If the list becomes empty after removal, reset hand
            auto node_to_erase = it->second;
            EvictionStrategy::RemoveKey(key);
            use_bits_.erase(key);

            if (all_key_list_.empty()) {
                clock_hand_ = all_key_list_.end();
            }
        }
        return ErrorCode::OK;
    }

    virtual std::string EvictKey(void) override {
        if (all_key_list_.empty()) {
            return "";
        }

        while (true) {
            std::string current_key = *clock_hand_;

            if (use_bits_[current_key]) {
                // Second chance granted: clear bit and move hand
                use_bits_[current_key] = false;
                AdvanceHand();
            } else {
                // Bit is 0: Evict this key
                std::string evicted_key = current_key;
                
                // Move hand forward before erasing the current node
                AdvanceHand();
                
                // Cleanup
                auto it = all_key_idx_map_.find(evicted_key);
                all_key_list_.erase(it->second);
                all_key_idx_map_.erase(it);
                use_bits_.erase(evicted_key);

                if (all_key_list_.empty()) {
                    clock_hand_ = all_key_list_.end();
                }
                
                return evicted_key;
            }
        }
    }

   private:
    void AdvanceHand() {
        if (all_key_list_.empty()) return;
        clock_hand_++;
        if (clock_hand_ == all_key_list_.end()) {
            clock_hand_ = all_key_list_.begin();
        }
    }

    std::unordered_map<std::string, bool> use_bits_;
    std::list<std::string>::iterator clock_hand_;
};

}  // namespace mooncake