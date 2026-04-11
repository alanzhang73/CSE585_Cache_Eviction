#pragma once

#include <chrono>
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

class EvictionPolicy {
   public:
    virtual ~EvictionPolicy() = default;

    virtual EvictionPolicyType type() const noexcept = 0;

    virtual std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) const = 0;
};

std::unique_ptr<EvictionPolicy> CreateEvictionPolicy(EvictionPolicyType type);

}  // namespace mooncake
