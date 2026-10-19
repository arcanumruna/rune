// CatalogPartition.cpp — the `__catalog` partition's compile-time pins (catalog history spec §3).
// The PartitionConfig itself is the generic admin-log one (AdminLogPartition.cpp, authentication plan D39).
#include <rune/node/CatalogPartition.h>
#include <rune/node/MetadataService.h>

#include <rune/protocol/client/Protocol.h>

namespace rune::node {

static_assert(kCatalogTopicId == rune::protocol::client::kCatalogTopicId,
              "the broker's __catalog topic id must equal the wire's");
static_assert(MetadataService::isControllerIsrInternalTopic(kCatalogTopicId),
              "__catalog is on the controller-ISR path (ruling 7)");

} // namespace rune::node
