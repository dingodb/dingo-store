
#include "fmt/core.h"
#include "gmock/gmock.h"
#include "meta_cache.h"

namespace dingodb {
namespace sdk {

static std::string HostPortToAddrStr(std::string ip, int port) { return fmt::format("{}:{}", ip, port); }

class MockMetaCache : public MetaCache {
 public:
  explicit MockMetaCache(std::shared_ptr<CoordinatorInteraction> coordinator_interaction)
      : MetaCache(coordinator_interaction) {}

  ~MockMetaCache() override = default;

  MOCK_METHOD(Status, SendScanRegionsRequest,
              (const pb::coordinator::ScanRegionsRequest& request, pb::coordinator::ScanRegionsResponse& response),
              (override));
};

}  // namespace sdk
}  // namespace dingodb