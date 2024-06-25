#pragma once

#include "LicenseCheck.h"
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace MyscaleLicense
{
using namespace DB;

class ClusterLicenseChecker : public ILicenseChecker
{
public:
    ClusterLicenseChecker(
        const Poco::Util::LayeredConfiguration & server_config_,
        const ContextMutablePtr global_context,
        const XMLDocumentPtr & preprocessed_xml_)
        : ILicenseChecker(server_config_, global_context, preprocessed_xml_)
        , zookeeper(getContext()->getZooKeeper())
        , cluster_name(getLicenseClusterName())
        , cluster_prefix(fmt::format(fmt::runtime(LICENSE_CLUSTER_PREFIX_FMT), cluster_name))
        , active_node_prefix(fmt::format(fmt::runtime(ACTIVE_NODES_PREFIX_FMT), cluster_name))
    {
    }

    ~ClusterLicenseChecker() override = default;

private:
    struct znode_info
    {
        String node_path;
        String node_value;
        int32_t mode;
    };

    String getLicenseClusterName() const;
    void offlineInstanceInZookeeper();

    void createClusterLicenseInfoIfNotExists();
    void beforeCheckLicense(const LicenseCheckCtx & check_ctx) override;
    void checkInstanceCount(const LicenseCheckCtx & check_ctx) override;
    void checkLicenseFinal() override;
    void stopLicenseCheckImpl() override {
        /// stop update instance status task
        if (update_instance_status_task)
            (*update_instance_status_task)->deactivate();
        offlineInstanceInZookeeper(); 
    }
    void updateInstanceStatus();

    zkutil::ZooKeeperPtr zookeeper;
    const String cluster_name;
    const String cluster_prefix;
    const String active_node_prefix;
    std::mutex instance_status_mutex;
    String instance_status_path;

    std::unique_ptr<BackgroundSchedulePoolTaskHolder> update_instance_status_task;
};

} // namespace MyscaleLicense
