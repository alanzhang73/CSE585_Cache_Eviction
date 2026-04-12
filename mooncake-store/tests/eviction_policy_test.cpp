#include <gtest/gtest.h>

#include <chrono>
#include <vector>

#include "eviction_policy.h"

namespace mooncake {
namespace {

EvictionCandidate Candidate(
    std::string key, bool recently_referenced,
    std::chrono::system_clock::time_point lease_timeout, size_t size = 1024) {
    EvictionCandidate candidate;
    candidate.key = std::move(key);
    candidate.recently_referenced = recently_referenced;
    candidate.lease_timeout = lease_timeout;
    candidate.size = size;
    return candidate;
}

}  // namespace

TEST(EvictionPolicyTest, SievePrefersUnreferencedCandidates) {
    auto policy = CreateEvictionPolicy(EvictionPolicyType::SIEVE);
    const auto now = std::chrono::system_clock::now();

    std::vector<EvictionCandidate> candidates = {
        Candidate("referenced-old", true, now - std::chrono::seconds(10)),
        Candidate("unreferenced-new", false, now - std::chrono::seconds(1)),
    };

    const auto victims = policy->SelectVictims(candidates, 1);
    ASSERT_EQ(victims.size(), 1);
    EXPECT_EQ(victims[0], "unreferenced-new");
}

TEST(EvictionPolicyTest, SieveFallsBackToLeaseTimeoutWithinSameReferenceClass) {
    auto policy = CreateEvictionPolicy(EvictionPolicyType::SIEVE);
    const auto now = std::chrono::system_clock::now();

    std::vector<EvictionCandidate> candidates = {
        Candidate("older", false, now - std::chrono::seconds(10)),
        Candidate("newer", false, now - std::chrono::seconds(1)),
    };

    const auto victims = policy->SelectVictims(candidates, 1);
    ASSERT_EQ(victims.size(), 1);
    EXPECT_EQ(victims[0], "older");
}

TEST(EvictionPolicyTest, OriginalIgnoresRecentlyReferencedBit) {
    auto policy = CreateEvictionPolicy(EvictionPolicyType::ORIGINAL);
    const auto now = std::chrono::system_clock::now();

    std::vector<EvictionCandidate> candidates = {
        Candidate("older-referenced", true, now - std::chrono::seconds(10)),
        Candidate("newer-unreferenced", false, now - std::chrono::seconds(1)),
    };

    const auto victims = policy->SelectVictims(candidates, 1);
    ASSERT_EQ(victims.size(), 1);
    EXPECT_EQ(victims[0], "older-referenced");
}

}  // namespace mooncake
