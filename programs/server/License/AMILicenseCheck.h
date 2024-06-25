#pragma once

#include <filesystem>

#include <Interpreters/Context.h>
#include <base/types.h>
#include <base/JSON.h>
#include <Poco/Base64Decoder.h>
#include <Poco/DOM/Text.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/XML/XMLWriter.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Daemon/BaseDaemon.h>

#if USE_SSL
#    include <openssl/pem.h>
#    include <openssl/rsa.h>
#    include <openssl/sha.h>
#    include <openssl/bio.h>
#    include <openssl/x509.h>
#else
#    error "License check is not supported without SSL."
#endif


namespace MyscaleLicense
{

using namespace DB;

/// Amazon EC2 metadata
const String AMAZON_MYSCALE_MARKETPLACE_PRODUCT_CODE = "dd4gh8by099sh0lxvok3lhtbk";
// const String AMAZON_EC2_METADATA_SERVER_IP = "127.0.0.1:1338";  /// for debug
const String AMAZON_EC2_METADATA_SERVER_IP = "169.254.169.254";
const String AMAZON_EC2_DYNAMIC_DATA_URL = "/latest/dynamic/instance-identity/document";
const String AMAZON_EC2_TOKEN_URL = "/latest/api/token";
const String AMAZON_EC2_SIGNATURE_URL = "/latest/dynamic/instance-identity/signature";
const String AMAZON_EC2_INSTANCE_ID_FILE = "/var/lib/cloud/data/instance-id";

struct AmazonInstanceMetadata
{
    String instance_id;
    String dynamic_document;
    String signature;

    static AmazonInstanceMetadata generateAmazonInstanceMetadata()
    {
        AmazonInstanceMetadata metadata;
        metadata.init();
        return metadata;
    }

    void checkAmazonDocumentSignature() const;
private:
    void init();
    String getAmazonHttpToken() const;
    String getAmazonDynamicDocument(const String & token) const;
    String getAmazonSignature(const String & token) const;
    String getAmazonInstanceId() const;
};

class AMILicenseChecker
{
public:
    AMILicenseChecker() = default;
    ~AMILicenseChecker() = default;

    void checkLicense()
    {
        /// check instance id from amazon
        try
        {
            auto amazon_metadata = AmazonInstanceMetadata::generateAmazonInstanceMetadata();

            /// check amazon document signature
            checkAmazonDocumentSignature(amazon_metadata);

            /// check instance id
            checkInstanceID(amazon_metadata);

            /// check marketplace product code
            checkMarketplaceProductCode(amazon_metadata);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "AMILicenseChecker check license failed. error: {}", e.displayText());
            BaseDaemon::terminate();
        }
        catch (...)
        {
            LOG_ERROR(log, "AMILicenseChecker check license failed. error: unknown error.");
            BaseDaemon::terminate();
        }
    }

private:
    void checkAmazonDocumentSignature(const AmazonInstanceMetadata & metadata);
    void checkInstanceID(const AmazonInstanceMetadata & metadata);
    void checkMarketplaceProductCode(const AmazonInstanceMetadata & metadata);

    Poco::Logger * log = &Poco::Logger::get("AMILicenseChecker");
};

} // namespace MyscaleLicense
