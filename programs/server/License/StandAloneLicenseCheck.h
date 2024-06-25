#pragma once

#include "LicenseCheck.h"

namespace MyscaleLicense
{

class StandAloneLicenseChecker : public ILicenseChecker
{
public:
    StandAloneLicenseChecker(
        const Poco::Util::LayeredConfiguration & server_config_,
        const ContextMutablePtr global_context,
        const XMLDocumentPtr & preprocessed_xml_)
        : ILicenseChecker(server_config_, global_context, preprocessed_xml_)
    {
    }
    ~StandAloneLicenseChecker() override = default;

private:
    void checkInstanceCount(const LicenseCheckCtx & check_ctx) override
    {
        int max_instance_count = std::stoi(check_ctx.license_doc->getNodeByPath(INSTANCE_COUNT_PATH_XML)->innerText());
        if (max_instance_count < 1)
        {
            LOG_ERROR(log, "The number of cluster instances in stand-alone mode is greater than 1: {}.", max_instance_count);
            throw Exception(
                ErrorCodes::LICENSE_ERROR, "Check license failed, the number of cluster instances in stand-alone mode is greater than 1.");
        }
    }
};

}
