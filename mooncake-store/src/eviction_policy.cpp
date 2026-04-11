#include "eviction_policy.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace mooncake {
namespace {

template <typename Compare>
std::vector<std::string> SelectTopVictims(
    const std::vector<EvictionCandidate>& candidates, size_t victim_count,
    Compare compare) {
    if (victim_count == 0 || candidates.empty()) {
        return {};
    }

    victim_count = std::min(victim_count, candidates.size());

    std::vector<const EvictionCandidate*> ordered;
    ordered.reserve(candidates.size());
    for (const auto& candidate : candidates) {
        ordered.push_back(&candidate);
    }

    std::partial_sort(ordered.begin(), ordered.begin() + victim_count,
                      ordered.end(),
                      [&](const EvictionCandidate* lhs,
                          const EvictionCandidate* rhs) {
                          return compare(*lhs, *rhs);
                      });

    std::vector<std::string> victims;
    victims.reserve(victim_count);
    for (size_t i = 0; i < victim_count; ++i) {
        victims.push_back(ordered[i]->key);
    }
    return victims;
}

bool HasDistinctScores(const std::optional<double>& lhs,
                       const std::optional<double>& rhs) {
    return lhs.has_value() && rhs.has_value() && lhs.value() != rhs.value();
}

class OriginalEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::ORIGINAL;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) const override {
        return SelectTopVictims(
            candidates, victim_count,
            [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
                if (lhs.lease_timeout != rhs.lease_timeout) {
                    return lhs.lease_timeout < rhs.lease_timeout;
                }
                if (lhs.size != rhs.size) {
                    return lhs.size > rhs.size;
                }
                return lhs.key < rhs.key;
            });
    }
};

class SizeAwareEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::SIZE_AWARE;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) const override {
        return SelectTopVictims(
            candidates, victim_count,
            [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
                if (lhs.size != rhs.size) {
                    return lhs.size > rhs.size;
                }
                if (lhs.lease_timeout != rhs.lease_timeout) {
                    return lhs.lease_timeout < rhs.lease_timeout;
                }
                return lhs.key < rhs.key;
            });
    }
};

class AttentionAwareEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::ATTENTION_AWARE;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) const override {
        return SelectTopVictims(
            candidates, victim_count,
            [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
                if (HasDistinctScores(lhs.attention_score, rhs.attention_score)) {
                    return lhs.attention_score.value() <
                           rhs.attention_score.value();
                }
                if (lhs.lease_timeout != rhs.lease_timeout) {
                    return lhs.lease_timeout < rhs.lease_timeout;
                }
                if (lhs.size != rhs.size) {
                    return lhs.size > rhs.size;
                }
                return lhs.key < rhs.key;
            });
    }
};

class ScoreBasedEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::SCORE_BASED;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) const override {
        return SelectTopVictims(
            candidates, victim_count,
            [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
                if (HasDistinctScores(lhs.future_utility_score,
                                      rhs.future_utility_score)) {
                    return lhs.future_utility_score.value() <
                           rhs.future_utility_score.value();
                }
                if (lhs.lease_timeout != rhs.lease_timeout) {
                    return lhs.lease_timeout < rhs.lease_timeout;
                }
                if (lhs.size != rhs.size) {
                    return lhs.size > rhs.size;
                }
                return lhs.key < rhs.key;
            });
    }
};

class LayerAwareEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::LAYER_AWARE;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) const override {
        return SelectTopVictims(
            candidates, victim_count,
            [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
                if (HasDistinctScores(lhs.layer_priority_score,
                                      rhs.layer_priority_score)) {
                    return lhs.layer_priority_score.value() <
                           rhs.layer_priority_score.value();
                }

                if (HasDistinctScores(lhs.attention_score,
                                      rhs.attention_score)) {
                    return lhs.attention_score.value() <
                           rhs.attention_score.value();
                }

                if (lhs.lease_timeout != rhs.lease_timeout) {
                    return lhs.lease_timeout < rhs.lease_timeout;
                }
                if (lhs.size != rhs.size) {
                    return lhs.size > rhs.size;
                }
                return lhs.key < rhs.key;
            });
    }
};

class SieveEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::SIEVE;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) const override {
        return SelectTopVictims(
            candidates, victim_count,
            [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
                if (lhs.recently_referenced != rhs.recently_referenced) {
                    return !lhs.recently_referenced;
                }
                if (lhs.lease_timeout != rhs.lease_timeout) {
                    return lhs.lease_timeout < rhs.lease_timeout;
                }
                if (lhs.size != rhs.size) {
                    return lhs.size > rhs.size;
                }
                return lhs.key < rhs.key;
            });
    }
};

}  // namespace

const char* ToString(EvictionPolicyType type) noexcept {
    switch (type) {
        case EvictionPolicyType::ORIGINAL:
            return "original";
        case EvictionPolicyType::SIZE_AWARE:
            return "size_aware";
        case EvictionPolicyType::ATTENTION_AWARE:
            return "attention_aware";
        case EvictionPolicyType::SCORE_BASED:
            return "score_based";
        case EvictionPolicyType::LAYER_AWARE:
            return "layer_aware";
        case EvictionPolicyType::SIEVE:
            return "sieve";
    }
    return "original";
}

EvictionPolicyType ParseEvictionPolicyType(const std::string& value) {
    std::string normalized = value;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (normalized == "original" || normalized == "baseline") {
        return EvictionPolicyType::ORIGINAL;
    }
    if (normalized == "size_aware" || normalized == "size-aware") {
        return EvictionPolicyType::SIZE_AWARE;
    }
    if (normalized == "attention_aware" || normalized == "attention-aware") {
        return EvictionPolicyType::ATTENTION_AWARE;
    }
    if (normalized == "score_based" || normalized == "score-based" ||
        normalized == "nacl") {
        return EvictionPolicyType::SCORE_BASED;
    }
    if (normalized == "layer_aware" || normalized == "layer-aware" ||
        normalized == "cake") {
        return EvictionPolicyType::LAYER_AWARE;
    }
    if (normalized == "sieve") {
        return EvictionPolicyType::SIEVE;
    }

    throw std::invalid_argument("Unknown eviction policy: " + value);
}

std::unique_ptr<EvictionPolicy> CreateEvictionPolicy(EvictionPolicyType type) {
    switch (type) {
        case EvictionPolicyType::ORIGINAL:
            return std::make_unique<OriginalEvictionPolicy>();
        case EvictionPolicyType::SIZE_AWARE:
            return std::make_unique<SizeAwareEvictionPolicy>();
        case EvictionPolicyType::ATTENTION_AWARE:
            return std::make_unique<AttentionAwareEvictionPolicy>();
        case EvictionPolicyType::SCORE_BASED:
            return std::make_unique<ScoreBasedEvictionPolicy>();
        case EvictionPolicyType::LAYER_AWARE:
            return std::make_unique<LayerAwareEvictionPolicy>();
        case EvictionPolicyType::SIEVE:
            return std::make_unique<SieveEvictionPolicy>();
    }

    return std::make_unique<OriginalEvictionPolicy>();
}

}  // namespace mooncake
