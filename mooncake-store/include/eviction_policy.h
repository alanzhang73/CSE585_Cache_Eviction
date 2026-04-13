#pragma once

#include <chrono>
#include <string_view>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mooncake {

enum class EvictionPolicyType {
    ORIGINAL = 0,
    SIZE_AWARE,
    ATTENTION_AWARE,
    SCORE_BASED,
    LAYER_AWARE,
    SIEVE,
};

const char* ToString(EvictionPolicyType type) noexcept;
EvictionPolicyType ParseEvictionPolicyType(const std::string& value);

struct EvictionCandidate {
    std::string key;
    std::chrono::system_clock::time_point lease_timeout{};
    size_t size{0};
    bool soft_pinned{false};
    std::optional<double> attention_score;
    std::optional<double> future_utility_score;
    std::optional<double> layer_priority_score;
    bool recently_referenced{false};
};

enum class EvictionActionType {
    EVICT = 0,
    CLEAR_REFERENCE_AND_SKIP,
};

struct EvictionAction {
    std::string key;
    EvictionActionType action{EvictionActionType::EVICT};
};

class EvictionPolicy {
   public:
    virtual ~EvictionPolicy() = default;

    virtual EvictionPolicyType type() const noexcept = 0;

    virtual bool uses_stateful_traversal() const noexcept { return false; }

    virtual void SynchronizeKeys(
        const std::vector<std::string>& current_keys) {}

    virtual std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) = 0;

    virtual std::vector<EvictionAction> SelectVictimActions(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count);
};

std::unique_ptr<EvictionPolicy> CreateEvictionPolicy(EvictionPolicyType type);

}  // namespace mooncake
