#include "eviction_policy.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <list>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace mooncake {
namespace {

std::string SummarizeVictims(const std::vector<std::string>& victims,
                             size_t limit = 5) {
    std::ostringstream oss;
    const size_t count = std::min(limit, victims.size());
    for (size_t i = 0; i < count; ++i) {
        if (i > 0) {
            oss << ",";
        }
        oss << victims[i];
    }
    if (victims.size() > limit) {
        oss << ",...";
    }
    return oss.str();
}

void LogPolicySelection(EvictionPolicyType policy_type,
                        const std::vector<EvictionCandidate>& candidates,
                        size_t requested_victim_count,
                        const std::vector<std::string>& victims) {
    size_t attention_score_count = 0;
    size_t future_utility_score_count = 0;
    size_t layer_priority_score_count = 0;
    size_t recently_referenced_count = 0;
    size_t soft_pinned_count = 0;
    for (const auto& candidate : candidates) {
        attention_score_count += candidate.attention_score.has_value();
        future_utility_score_count += candidate.future_utility_score.has_value();
        layer_priority_score_count += candidate.layer_priority_score.has_value();
        recently_referenced_count += candidate.recently_referenced;
        soft_pinned_count += candidate.soft_pinned;
    }

    LOG(INFO) << "action=eviction_policy_select"
              << ", policy=" << ToString(policy_type)
              << ", candidates=" << candidates.size()
              << ", requested_victim_count=" << requested_victim_count
              << ", selected_victims=" << victims.size()
              << ", soft_pinned_candidates=" << soft_pinned_count
              << ", attention_score_candidates=" << attention_score_count
              << ", future_utility_score_candidates="
              << future_utility_score_count
              << ", layer_priority_score_candidates="
              << layer_priority_score_count
              << ", recently_referenced_candidates="
              << recently_referenced_count
              << ", victims=[" << SummarizeVictims(victims) << "]";
}

void LogPolicyActions(EvictionPolicyType policy_type,
                      const std::vector<EvictionCandidate>& candidates,
                      size_t requested_victim_count,
                      const std::vector<EvictionAction>& actions) {
    std::vector<std::string> victims;
    victims.reserve(actions.size());
    size_t clear_reference_count = 0;
    for (const auto& action : actions) {
        if (action.action == EvictionActionType::EVICT) {
            victims.push_back(action.key);
        } else {
            ++clear_reference_count;
        }
    }

    LOG(INFO) << "action=eviction_policy_actions"
              << ", policy=" << ToString(policy_type)
              << ", candidates=" << candidates.size()
              << ", requested_victim_count=" << requested_victim_count
              << ", selected_victims=" << victims.size()
              << ", clear_reference_actions=" << clear_reference_count
              << ", victims=[" << SummarizeVictims(victims) << "]";
}

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
        size_t victim_count) override {
        auto victims = SelectTopVictims(
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
        LogPolicySelection(type(), candidates, victim_count, victims);
        return victims;
    }
};

class SizeAwareEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::SIZE_AWARE;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) override {
        auto victims = SelectTopVictims(
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
        LogPolicySelection(type(), candidates, victim_count, victims);
        return victims;
    }
};

class AttentionAwareEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::ATTENTION_AWARE;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) override {
        auto victims = SelectTopVictims(
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
        LogPolicySelection(type(), candidates, victim_count, victims);
        return victims;
    }
};

class ScoreBasedEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::SCORE_BASED;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) override {
        auto victims = SelectTopVictims(
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
        LogPolicySelection(type(), candidates, victim_count, victims);
        return victims;
    }
};

class LayerAwareEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::LAYER_AWARE;
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) override {
        auto victims = SelectTopVictims(
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
        LogPolicySelection(type(), candidates, victim_count, victims);
        return victims;
    }
};

class SieveEvictionPolicy final : public EvictionPolicy {
   public:
    EvictionPolicyType type() const noexcept override {
        return EvictionPolicyType::SIEVE;
    }

    bool uses_stateful_traversal() const noexcept override { return true; }

    void SynchronizeKeys(
        const std::vector<std::string>& current_keys) override {
        std::unordered_set<std::string> current(current_keys.begin(),
                                               current_keys.end());

        for (auto it = queue_.begin(); it != queue_.end();) {
            if (!current.contains(*it)) {
                positions_.erase(*it);
                if (hand_ == it) {
                    hand_ = queue_.erase(it);
                } else {
                    it = queue_.erase(it);
                }
                continue;
            }
            ++it;
        }

        for (const auto& key : current_keys) {
            if (positions_.contains(key)) {
                continue;
            }
            queue_.push_back(key);
            auto inserted = std::prev(queue_.end());
            positions_.emplace(key, inserted);
            if (hand_ == queue_.end()) {
                hand_ = queue_.begin();
            }
        }

        if (queue_.empty()) {
            hand_ = queue_.end();
        } else if (hand_ == queue_.end()) {
            hand_ = queue_.begin();
        }
    }

    std::vector<std::string> SelectVictims(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) override {
        auto victims = SelectTopVictims(
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
        LogPolicySelection(type(), candidates, victim_count, victims);
        return victims;
    }

    std::vector<EvictionAction> SelectVictimActions(
        const std::vector<EvictionCandidate>& candidates,
        size_t victim_count) override {
        if (victim_count == 0 || candidates.empty() || queue_.empty()) {
            return {};
        }

        std::unordered_map<std::string, const EvictionCandidate*> by_key;
        by_key.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            by_key.emplace(candidate.key, &candidate);
        }

        std::vector<EvictionAction> actions;
        actions.reserve(victim_count * 2);
        const size_t max_scan = std::max(queue_.size(), victim_count * 2);
        size_t scanned = 0;
        size_t evicted = 0;

        while (!queue_.empty() && scanned < max_scan && evicted < victim_count) {
            if (hand_ == queue_.end()) {
                hand_ = queue_.begin();
            }
            if (hand_ == queue_.end()) {
                break;
            }

            auto current = hand_++;
            ++scanned;

            auto candidate_it = by_key.find(*current);
            if (candidate_it == by_key.end()) {
                continue;
            }

            const auto& candidate = *candidate_it->second;
            if (candidate.recently_referenced) {
                actions.push_back(
                    {candidate.key, EvictionActionType::CLEAR_REFERENCE_AND_SKIP});
                continue;
            }

            actions.push_back({candidate.key, EvictionActionType::EVICT});
            ++evicted;
        }

        LogPolicyActions(type(), candidates, victim_count, actions);
        return actions;
    }

   private:
    std::list<std::string> queue_;
    std::unordered_map<std::string, std::list<std::string>::iterator> positions_;
    std::list<std::string>::iterator hand_{queue_.end()};
};

}  // namespace

std::vector<EvictionAction> EvictionPolicy::SelectVictimActions(
    const std::vector<EvictionCandidate>& candidates, size_t victim_count) {
    std::vector<EvictionAction> actions;
    for (auto& key : SelectVictims(candidates, victim_count)) {
        actions.push_back({std::move(key), EvictionActionType::EVICT});
    }
    return actions;
}

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
