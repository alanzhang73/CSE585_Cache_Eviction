#include "master_service_test_for_snapshot_base.h"

#include <glog/logging.h>

namespace mooncake::test {
namespace {

class MasterServiceRadixTreeTest : public MasterServiceSnapshotTestBase {
   protected:
    void SetUp() override {
        MasterServiceSnapshotTestBase::SetUp();
        google::InitGoogleLogging("MasterServiceRadixTreeTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override {
        service_.reset();
        google::ShutdownGoogleLogging();
    }

    void InitService(EvictionPolicyType policy_type = EvictionPolicyType::ORIGINAL) {
        auto config = MasterServiceConfig::builder()
                          .set_memory_allocator(BufferAllocatorType::OFFSET)
                          .set_eviction_policy_type(policy_type)
                          .set_default_kv_lease_ttl(1)
                          .build();
        service_ = std::make_unique<MasterService>(config);
        segment_ctx_ = PrepareSimpleSegment(*service_);
    }

    void PutObject(const std::string& key,
                   std::vector<std::string> path_segments,
                   std::optional<std::string> parent_key = std::nullopt,
                   uint64_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.radix_path_segments = std::move(path_segments);
        config.radix_parent_key = std::move(parent_key);
        auto put_start = service_->PutStart(segment_ctx_.client_id, key, size, config);
        ASSERT_TRUE(put_start.has_value());
        auto put_end =
            service_->PutEnd(segment_ctx_.client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value());
    }

    std::unique_ptr<MasterService> service_;
    MountedSegmentContext segment_ctx_;
};

TEST_F(MasterServiceRadixTreeTest, LeafFirstDeletionBlocksParentClear) {
    InitService();
    PutObject("root", {"root"});
    PutObject("child", {"child"}, "root");

    const auto expired_time =
        std::chrono::system_clock::now() - std::chrono::seconds(10);
    SetLeaseTimeoutForTest(service_.get(), "root", expired_time);
    SetLeaseTimeoutForTest(service_.get(), "child", expired_time);

    auto clear_root =
        service_->BatchReplicaClear({"root"}, segment_ctx_.client_id, "");
    ASSERT_TRUE(clear_root.has_value());
    EXPECT_TRUE(clear_root->empty());
    EXPECT_TRUE(ObjectExists(service_.get(), "root"));
    EXPECT_TRUE(ObjectExists(service_.get(), "child"));

    auto clear_child =
        service_->BatchReplicaClear({"child"}, segment_ctx_.client_id, "");
    ASSERT_TRUE(clear_child.has_value());
    ASSERT_EQ(clear_child->size(), 1);
    EXPECT_EQ((*clear_child)[0], "child");

    auto clear_root_again =
        service_->BatchReplicaClear({"root"}, segment_ctx_.client_id, "");
    ASSERT_TRUE(clear_root_again.has_value());
    ASSERT_EQ(clear_root_again->size(), 1);
    EXPECT_EQ((*clear_root_again)[0], "root");
}

TEST_F(MasterServiceRadixTreeTest, PutStartRejectsMissingRadixParent) {
    InitService();

    ReplicateConfig config;
    config.replica_num = 1;
    config.radix_parent_key = "missing-parent";
    config.radix_path_segments = {"child"};

    auto put_start =
        service_->PutStart(segment_ctx_.client_id, "orphan", 1024, config);
    ASSERT_FALSE(put_start.has_value());
    EXPECT_EQ(put_start.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(ObjectExists(service_.get(), "orphan"));
}

TEST_F(MasterServiceRadixTreeTest,
       OriginalPolicyRanksOnlyEligibleLeavesAndSkipsInternalPrefixNodes) {
    InitService(EvictionPolicyType::ORIGINAL);
    PutObject("root", {"root"});
    PutObject("child-a", {"a"}, "root");
    PutObject("child-b", {"b"}, "root");

    const auto old_time =
        std::chrono::system_clock::now() - std::chrono::seconds(10);
    const auto newer_time =
        std::chrono::system_clock::now() - std::chrono::milliseconds(100);
    SetLeaseTimeoutForTest(service_.get(), "root", old_time);
    SetLeaseTimeoutForTest(service_.get(), "child-a", old_time);
    SetLeaseTimeoutForTest(service_.get(), "child-b", newer_time);

    CallBatchEvict(service_.get(), 0.20, 0.20);

    EXPECT_TRUE(ObjectExists(service_.get(), "root"));

    auto child_a = service_->GetReplicaList("child-a");
    ASSERT_FALSE(child_a.has_value());
    EXPECT_EQ(child_a.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_TRUE(service_->GetReplicaList("child-b").has_value());
}

}  // namespace
}  // namespace mooncake::test
