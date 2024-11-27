#pragma once

#include <filesystem>
#include <memory>

#include <iostream>
#include <sstream>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <base/types.h>
#include <base/JSON.h>
#include <Poco/Base64Decoder.h>
#include <Poco/DOM/Text.h>
#include <Poco/Exception.h>
#include <Poco/Logger.h>
#include <Poco/MemoryStream.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/XML/XMLWriter.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Daemon/BaseDaemon.h>
#include "LicenseUtil.h"

#if USE_SSL
#    include <openssl/pem.h>
#    include <openssl/rsa.h>
#    include <openssl/sha.h>
#else
#    error "License check is not supported without SSL."
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LICENSE_ERROR;
    extern const int LIMIT_EXCEEDED;
}
}

namespace MyscaleLicense
{
using namespace DB;
const size_t RETRY_TIMES = 3;
const size_t RETRY_INTERVAL_S = 1200;
const size_t MAX_CHECK_PERIOD = 86400;
const size_t UPDATE_INSTANCE_STATUS_INTERVAL_S = 60;

const String LICENSE_CLUSTERS_PREFIX = "/license_clusters";
const String LICENSE_CLUSTER_PREFIX_FMT = "/license_clusters/{}";
const String ACTIVE_NODES_PREFIX_FMT = "/license_clusters/{}/active_nodes";
const String ACTIVE_NODE_FMT = "/license_clusters/{}/active_nodes/{}";
const String ACTIVE_INSTANCE_FMT = "/license_clusters/{}/active_nodes/{}/{}";

const String LICENSE_PATH_XML = "/license";
const String LICENSE_INFO_PATH_XML = "/license/license_info";
const String CLUSTER_NAME_PATH_XML = "/license/license_info/cluster_name";
const String INSTANCE_COUNT_PATH_XML = "/license/license_info/instance_count";
const String EXPIRATION_PATH_XML = "/license/license_info/expiration";
const String LICENSE_SIGN_PATH_XML = "/license/license_signature";
const String LICENSE_PUBLIC_KEY_PATH_XML = "/license/license_public_key";
const String CPU_COUNT_PATH_XML = "/license/license_info/instance_cpu_count";
const String MEMORY_AMOUNT_PATH_XML = "/license/license_info/instance_memory_amount";
const String NODES_INFO_PATH_XML = "/license/license_info/nodes_info";

const String LICENSE_TAG = "license";
const String LICENSE_INFO_TAG = "license_info";
const String CLUSTER_NAME_TAG = "cluster_name";
const String INSTANCE_COUNT_TAG = "instance_count";
const String INSTANCE_CPU_TAG = "instance_cpu_count";
const String INSTANCE_MEMORY_TAG = "instance_memory_amount";
const String NODES_INFO_TAG = "nodes_info";
const String NODE_INFO_TAG = "node_info";
const String MACHINE_ID_TAG = "given_id";
const String SYSTEM_UUID_TAG = "machine_info";
const String EXPIRED_DATE_TAG = "expiration";
const String LICENSE_SIGNATURE_TAG = "license_signature";
const String LICENSE_PUBLIC_KEY_TAG = "license_public_key";

const String LICENSE_FILE_NAME = "license.xml";
const String RSA_PUBLIC_KEY_FILE_NAME = "rsa_public_key.pem";

class ILicenseChecker : public WithMutableContext
{
public:
    using Node = Poco::XML::Node;
    using Element = Poco::XML::Element;
    using ElementPtr = Poco::AutoPtr<Poco::XML::Element>;
    using TextPtr = Poco::AutoPtr<Poco::XML::Text>;

public:
    ILicenseChecker(
        const Poco::Util::LayeredConfiguration & server_config_,
        const ContextMutablePtr global_context,
        const XMLDocumentPtr & preprocessed_xml);
    virtual ~ILicenseChecker() = default;
    void scheduleLicenseCheckTask()
    {
        if (!license_task)
        {
            auto task_holder = getContext()->getSchedulePool().createTask("LicenseCheck", [this]() { this->CheckLicenseTask(); });
            license_task = std::make_unique<BackgroundSchedulePoolTaskHolder>(std::move(task_holder));
            (*license_task)->activate();
            (*license_task)->schedule();
        }
        else
            (*license_task)->activateAndSchedule();
    }

    /// license check task catch exception and terminate the process
    void CheckLicenseTask();

    void reloadLicenseInfo(const XMLDocumentPtr & preprocessed_xml);

    bool checkReloadLicenseModifyValid(const XMLDocumentPtr & current_license_doc, const XMLDocumentPtr & reload_license_doc);

    void stopLicenseCheckTask()
    {
        LOG_DEBUG(log, "Stop license check task.");
        if (license_task)
            (*license_task)->deactivate();
        stopLicenseCheckImpl();
    }

public:
    struct LicenseCheckCtx
    {
        LicenseCheckCtx(const XMLDocumentPtr license_doc_, bool check_fail_terminate_, bool renew_instance_status_)
            : license_doc(license_doc_), check_fail_terminate(check_fail_terminate_), renew_instance_status(renew_instance_status_)
        {
        }
        const XMLDocumentPtr license_doc;
        bool check_fail_terminate;
        mutable bool renew_instance_status;
    };
    using LicenseCheckCtxPtr = std::unique_ptr<LicenseCheckCtx>;

protected:
    String getLicenseFilePathPrefix(const Poco::Util::AbstractConfiguration & config) const;
    String getLicenseFileContent(const String & path) const;
    XMLDocumentPtr getFormatLicenseInfoDoc(XMLDocumentPtr license_doc_, bool new_format_license) const;
    XMLDocumentPtr getLicenseDoc(const XMLDocumentPtr & preprocessed_xml, bool need_format = true) const;
    LicenseCheckCtxPtr prepareCheckLicenseCtx(const XMLDocumentPtr current_license_doc, bool reload_config_check = false)
    {
        LicenseCheckCtxPtr ctx = std::make_unique<LicenseCheckCtx>(current_license_doc, !reload_config_check, false);
        return ctx;
    }
    /// check method desined throw exception to terminate the process
    virtual void beforeCheckLicense(const LicenseCheckCtx & /*check_ctx*/) { /*nothing to do*/}
    void checkLicenseSign(const LicenseCheckCtx & check_ctx);
    void checkMachineResource(const LicenseCheckCtx & check_ctx);
    void checkExpiration(const LicenseCheckCtx & check_ctx);
    void checkMachineInfo(const LicenseCheckCtx & check_ctx);
    virtual void checkZKLicenseInfo() { LOG_DEBUG(log, "No need to check zk license info."); }
    virtual void checkInstanceCount(const LicenseCheckCtx & check_ctx) = 0;
    virtual void checkLicenseFinal() { LOG_DEBUG(log, "License check is passed."); }
    virtual void stopLicenseCheckImpl() { /*nothing to do*/ }
    virtual void checkLicenseImpl(const LicenseCheckCtx & check_ctx)
    {
        beforeCheckLicense(check_ctx);

        LOG_DEBUG(log, "Start checking license sign.");
        checkLicenseSign(check_ctx);

        LOG_DEBUG(log, "Start checking zk license info.");
        checkZKLicenseInfo();

        LOG_DEBUG(log, "Start checking instance count.");
        checkInstanceCount(check_ctx);

        LOG_DEBUG(log, "Start checking machine info.");
        checkMachineInfo(check_ctx);

        LOG_DEBUG(log, "Start checking machine resource.");
        checkMachineResource(check_ctx);

        LOG_DEBUG(log, "Start checking license expiration.");
        checkExpiration(check_ctx);

        LOG_DEBUG(log, "Start final check.");
        checkLicenseFinal();
    }

    LoggerPtr log = getLogger("LicenseChecker");

    const Poco::Util::LayeredConfiguration & server_config;
    /// Does not support dynamic configuration changes
    const bool kubeconfig_enabled;
    const String machine_id_digest;
    const String system_uuid_digest;
    const String server_uuid_digest;
    const unsigned int cpu_count;
    const uint64_t memory_amount;
    const String public_key_path;

    std::mutex license_mutex;
    XMLDocumentPtr license_doc;
    size_t retry_times;
    std::unique_ptr<BackgroundSchedulePoolTaskHolder> license_task;
};

} // namespace MyscaleLicense
