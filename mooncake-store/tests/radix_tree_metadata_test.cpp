#include "radix_tree_metadata.h"

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

TEST(RadixTreeMetadataTest, TracksSharedPrefixesAndLeafObjects) {
    RadixTreeMetadataIndex index;

    index.RegisterObject("root", {"p0"});
    index.RegisterObject("child-a", {"p0", "a"});
    index.RegisterObject("child-b", {"p0", "b"});

    EXPECT_FALSE(index.IsLeafObject("root"));
    EXPECT_TRUE(index.IsLeafObject("child-a"));
    EXPECT_TRUE(index.IsLeafObject("child-b"));
}

TEST(RadixTreeMetadataTest, UnregisterPrunesOrphanedSuffixes) {
    RadixTreeMetadataIndex index;

    index.RegisterObject("root", {"p0"});
    index.RegisterObject("child", {"p0", "a"});

    EXPECT_FALSE(index.IsLeafObject("root"));
    index.UnregisterObject("child");
    EXPECT_FALSE(index.HasObject("child"));
    EXPECT_TRUE(index.IsLeafObject("root"));
}

}  // namespace
}  // namespace mooncake::test
