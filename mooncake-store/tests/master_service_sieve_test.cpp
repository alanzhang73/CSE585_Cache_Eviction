#include "master_service_test_for_snapshot_base.h"

#include <glog/logging.h>

namespace mooncake::test {
namespace {

class MasterServiceSieveTest : public MasterServiceSnapshotTestBase {
   protected:
    void SetUp() override {
        MasterServiceSnapshotTestBase::SetUp();
        google::InitGoogleLogging("MasterServiceSieveTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override {
        service_.reset();
        google::ShutdownGoogleLogging();
    }

    void InitService(EvictionPolicyType policy_type) {
        auto config = MasterServiceConfig::builder()
                          .set_memory_allocator(BufferAllocatorType::OFFSET)
                          .set_eviction_policy_type(policy_type)
                          .set_default_kv_lease_ttl(1)
                          .build();
        service_ = std::make_unique<MasterService>(config);
        segment_ctx_ = PrepareSimpleSegment(*service_);
    }

    void PutObject(const std::string& key, uint64_t size = 1024) {
        auto put_start =
            service_->PutStart(segment_ctx_.client_id, key, size, {.replica_num = 1});
        ASSERT_TRUE(put_start.has_value());
        auto put_end =
            service_->PutEnd(segment_ctx_.client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value());
    }

    std::unique_ptr<MasterService> service_;
    MountedSegmentContext segment_ctx_;
};

TEST_F(MasterServiceSieveTest, SuccessfulGetReplicaListMarksRecentlyReferenced) {
    InitService(EvictionPolicyType::SIEVE);
    PutObject("key");

    EXPECT_FALSE(IsRecentlyReferenced(service_.get(), "key"));

    auto result = service_->GetReplicaList("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(IsRecentlyReferenced(service_.get(), "key"));
}

TEST_F(MasterServiceSieveTest, FailedGetReplicaListDoesNotMarkOtherObjects) {
    InitService(EvictionPolicyType::SIEVE);
    PutObject("existing");

    auto result = service_->GetReplicaList("missing");
    ASSERT_FALSE(result.has_value());
    EXPECT_FALSE(IsRecentlyReferenced(service_.get(), "existing"));
}

TEST_F(MasterServiceSieveTest, GetReplicaListByRegexDoesNotMarkRecentlyReferenced) {
    InitService(EvictionPolicyType::SIEVE);
    PutObject("regex-key");

    auto result = service_->GetReplicaListByRegex("regex-.*");
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->find("regex-key") != result->end());
    EXPECT_FALSE(IsRecentlyReferenced(service_.get(), "regex-key"));
}

TEST_F(MasterServiceSieveTest,
       SieveBatchEvictPrefersUnreferencedWhenLeaseAndSizeMatch) {
    InitService(EvictionPolicyType::SIEVE);
    PutObject("a");
    PutObject("b");

    auto get_result = service_->GetReplicaList("a");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_TRUE(IsRecentlyReferenced(service_.get(), "a"));

    const auto expired_time =
        std::chrono::system_clock::now() - std::chrono::seconds(1);
    SetLeaseTimeoutForTest(service_.get(), "a", expired_time);
    SetLeaseTimeoutForTest(service_.get(), "b", expired_time);

    CallBatchEvict(service_.get(), 0.5, 0.5);

    EXPECT_TRUE(service_->GetReplicaList("a").has_value());
    auto evicted = service_->GetReplicaList("b");
    ASSERT_FALSE(evicted.has_value());
    EXPECT_EQ(evicted.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServiceSieveTest,
       OriginalBatchEvictFallsBackToKeyOrderWhenLeaseAndSizeMatch) {
    InitService(EvictionPolicyType::ORIGINAL);
    PutObject("a");
    PutObject("b");

    auto get_result = service_->GetReplicaList("a");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_TRUE(IsRecentlyReferenced(service_.get(), "a"));

    const auto expired_time =
        std::chrono::system_clock::now() - std::chrono::seconds(1);
    SetLeaseTimeoutForTest(service_.get(), "a", expired_time);
    SetLeaseTimeoutForTest(service_.get(), "b", expired_time);

    CallBatchEvict(service_.get(), 0.5, 0.5);

    auto evicted = service_->GetReplicaList("a");
    ASSERT_FALSE(evicted.has_value());
    EXPECT_EQ(evicted.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_TRUE(service_->GetReplicaList("b").has_value());
}

}  // namespace
}  // namespace mooncake::test
