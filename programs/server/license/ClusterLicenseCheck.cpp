#include "ClusterLicenseCheck.h"

namespace MyscaleLicense
{

String ClusterLicenseChecker::getLicenseClusterName() const
{
    String cluster_name_tmp;

    String key = CLUSTER_NAME_PATH_XML;
    std::replace(key.begin(), key.end(), '/', '.');
    trimLeft(key, '.');

    if (server_config.has(key))
        cluster_name_tmp = server_config.getString(key);
    else
    {
        auto * cluster_name_node = license_doc->getNodeByPath(CLUSTER_NAME_PATH_XML);
        if (cluster_name_node)
            cluster_name_tmp = cluster_name_node->innerText();
    }

    trim(cluster_name_tmp);
    if (cluster_name_tmp.empty())
    {
        LOG_ERROR(log, "Get license cluster name failed, please check the XML structure of license.");
        throw Exception(ErrorCodes::LICENSE_ERROR, "Get license cluster name failed, the XML structure of license may be incorrect.");
    }
    return cluster_name_tmp;
}

void ClusterLicenseChecker::createClusterLicenseInfoIfNotExists()
{
    std::vector<znode_info> znodes
        = {{LICENSE_CLUSTERS_PREFIX, "", zkutil::CreateMode::Persistent},
           {cluster_prefix, "", zkutil::CreateMode::Persistent},
           {active_node_prefix, "", zkutil::CreateMode::Persistent}};

    for (const auto & znode : znodes)
    {
        if (!zookeeper->exists(znode.node_path))
        {
            Coordination::Requests ops;
            ops.emplace_back(zkutil::makeCreateRequest(znode.node_path, znode.node_value, znode.mode));
            Coordination::Responses responses;
            auto code = zookeeper->tryMulti(ops, responses);
            if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
            {
                LOG_ERROR(log, "Can't create zknode in Zookeeper: {}.", Coordination::errorMessage(code));
                throw Exception(ErrorCodes::LICENSE_ERROR, "Can't create zknode in Zookeeper.");
            }
        }
    }
}

void ClusterLicenseChecker::beforeCheckLicense(const LicenseCheckCtx & check_ctx)
{
    if (zookeeper->expired())
    {
        LOG_WARNING(log, "Zookeeper connection is expired, renew the connection.");
        std::atomic_store(&zookeeper, getContext()->getZooKeeper());
        check_ctx.renew_instance_status = true;
        if (!instance_status_path.empty())
        {
            /// remove active node
            LOG_INFO(log, "Check active node: {}.", instance_status_path);
            if (zookeeper->exists(instance_status_path))
                LOG_INFO(log, "Remove active node: {}.", instance_status_path);
            zookeeper->deleteEphemeralNodeIfContentMatches(instance_status_path, "");
        }
    }
    createClusterLicenseInfoIfNotExists();
}

void ClusterLicenseChecker::checkInstanceCount(const LicenseCheckCtx & check_ctx)
{
    int max_instance_count = std::stoi(check_ctx.license_doc->getNodeByPath(INSTANCE_COUNT_PATH_XML)->innerText());
    int active_instance_count = 0;
    String active_instance_path = fmt::format(fmt::runtime(ACTIVE_INSTANCE_FMT), cluster_name, machine_id_digest, server_uuid_digest);
    Strings active_nodes = zookeeper->getChildren(active_node_prefix);
    for (auto node : active_nodes)
    {
        String node_path = active_node_prefix + "/" + node;
        Strings active_instances = zookeeper->getChildren(node_path);
        for (const auto & active_instance : active_instances)
        {
            if (zookeeper->exists(node_path + "/" + active_instance + "/active"))
                ++active_instance_count;
        }
    }
    active_instance_count = zookeeper->exists(active_instance_path + "/active") ? active_instance_count - 1 : active_instance_count;
    if (active_instance_count < max_instance_count)
    {
        LOG_DEBUG(
            log,
            "The number of cluster instances is checked, total number: {}, current number: {}.",
            max_instance_count,
            active_instance_count);
    }
    else
    {
        LOG_ERROR(log, "The number of cluster instances has reached the max value: {}.", max_instance_count);
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, the number of cluster instances has reached the max value.");
    }
}

void ClusterLicenseChecker::checkLicenseFinal()
{
    LOG_DEBUG(log, "License check is passed. Add active node to zookeeper.");
    String active_node_path = fmt::format(fmt::runtime(ACTIVE_NODE_FMT), cluster_name, machine_id_digest);
    String active_instance_path = fmt::format(fmt::runtime(ACTIVE_INSTANCE_FMT), cluster_name, machine_id_digest, server_uuid_digest);
    String active_path = active_instance_path + "/active";
    std::vector<znode_info> znodes
        = {{active_node_path, system_uuid_digest, zkutil::CreateMode::Persistent},
           {active_instance_path, "", zkutil::CreateMode::Persistent},
           {active_path, "", zkutil::CreateMode::Ephemeral}};
    for (const auto & znode : znodes)
    {
        if (!zookeeper->exists(znode.node_path))
        {
            auto code = zookeeper->tryCreate(znode.node_path, znode.node_value, znode.mode);
            if (code != Coordination::Error::ZOK && code != Coordination::Error::ZNODEEXISTS)
            {
                LOG_ERROR(log, "Can't create zknode in Zookeeper: {}.", Coordination::errorMessage(code));
                throw Exception(ErrorCodes::LICENSE_ERROR, "Can't create zknode in Zookeeper.");
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(instance_status_mutex);
        instance_status_path = active_path;
    }

    if (!update_instance_status_task)
    {
        auto task_holder = getContext()->getSchedulePool().createTask("UpdateInstanceStatus", [this]() { this->updateInstanceStatus(); });
        update_instance_status_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
        (*update_instance_status_task)->activate();
        (*update_instance_status_task)->schedule();
    }
}

void ClusterLicenseChecker::updateInstanceStatus()
{
    std::lock_guard<std::mutex> lock(instance_status_mutex);
    if (instance_status_path.empty())
        return;
    try
    {
        auto cur_zookeeper = std::atomic_load(&zookeeper);
        if (cur_zookeeper->expired())
        {
            LOG_DEBUG(log, "Renew Instance status path {} due to zookeeper session expired.", instance_status_path);
            cur_zookeeper = getContext()->getZooKeeper();
            cur_zookeeper->deleteEphemeralNodeIfContentMatches(instance_status_path, "");
            cur_zookeeper->tryCreate(instance_status_path, "", zkutil::CreateMode::Ephemeral);
        }
        else if (!cur_zookeeper->exists(instance_status_path))
        {
            LOG_DEBUG(log, "Instance status path {} not exists, try to create it.", instance_status_path);
            cur_zookeeper->tryCreate(instance_status_path, "", zkutil::CreateMode::Ephemeral);
        }
    }
    catch (...)
    {
        tryLogCurrentException("updateInstanceStatus");
    }
    (*update_instance_status_task)->scheduleAfter(UPDATE_INSTANCE_STATUS_INTERVAL_S * 1000);
}

void ClusterLicenseChecker::offlineInstanceInZookeeper()
{
    try
    {
        LOG_DEBUG(log, "Offline instance from Zookeeper.");
        String active_instance_path = fmt::format(fmt::runtime(ACTIVE_INSTANCE_FMT), cluster_name, machine_id_digest, server_uuid_digest);

        zookeeper->removeRecursive(active_instance_path);
    }
    catch (...)
    {
        tryLogCurrentException("offlineInstanceInZookeeper");
    }
}

}
