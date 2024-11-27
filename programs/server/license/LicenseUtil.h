#pragma once

#include <filesystem>

#include <base/getMemoryAmount.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/ShellCommand.h>
#include <Common/StringUtils.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/logger_useful.h>
#include <Daemon/BaseDaemon.h>
#include <IO/ReadHelpers.h>

#include <Poco/Base64Decoder.h>
#include <Poco/Logger.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Object.h>

#if USE_SSL
#    include <openssl/pem.h>
#    include <openssl/rsa.h>
#    include <openssl/sha.h>
#    include <openssl/err.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int LICENSE_ERROR;
}
}

namespace fs = std::filesystem;

namespace MyscaleLicense
{

const size_t MAX_CPU_COMMUNITY_EDITION = 4;
const uint64_t MAX_MEMORY_COMMUNITY_EDITION = 8589934592;

const String STRING_SUFFIX_FOR_DIGEST = "EMOSEWA BDQM !enignE-BD";

inline String getHexDigest(const String & content)
{
#if USE_SSL
    String salt_content = content + STRING_SUFFIX_FOR_DIGEST;
    unsigned char digest[33];
    SHA256(reinterpret_cast<const uint8_t *>(salt_content.c_str()), salt_content.size(), digest);
    int len = 32;
    String result;
    result.resize(2 * len);
    char hex_table[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    unsigned char * p_digest = digest;
    int index = 0;
    while (len--)
    {
        result[index++] = hex_table[*p_digest >> 4];
        result[index++] = hex_table[*(p_digest++) & 0x0F];
    }
    return result;
#else
    throw DB::Exception(ErrorCodes::LICENSE_ERROR, "Get hex digest failed, OpenSSL is not enabled.");
#endif
}

inline String getMachineIDDigest()
{
    String machine_id;
    auto command = DB::ShellCommand::execute("cat /etc/machine-id");
    readStringUntilEOF(machine_id, command->out);
    trimRight(machine_id, '\n');
    return getHexDigest(machine_id);
}

inline String getSystemUUIDDigest()
{
    String system_uuid = "";
    fs::path uuid_path = {"/sys/class/dmi/id/product_uuid"};
    if (fs::exists(uuid_path) && !fs::is_directory(uuid_path))
    {
        auto command = DB::ShellCommand::execute("cat " + uuid_path.string());
        readStringUntilEOF(system_uuid, command->out);
        trimRight(system_uuid, '\n');
    }
    return getHexDigest(system_uuid);
}

inline String base64Decode(const String & encoded)
{
    String decoded;
    Poco::MemoryInputStream istr(encoded.data(), encoded.size());
    Poco::Base64Decoder decoder(istr);
    Poco::StreamCopier::copyToString(decoder, decoded);
    return decoded;
}

inline void checkHardwareResourceLimitsForCommunityEdition()
{
    LoggerPtr log = getLogger("CommunityEditionLicenseChecker");
    LOG_DEBUG(log, "Start checking hardware resource limits of community edition");

    try
    {
        /// Check CPU and memory
        auto cpu_count = getNumberOfPhysicalCPUCores();
        auto memory_amount = getMemoryAmount();

        if (cpu_count <= MAX_CPU_COMMUNITY_EDITION)
            LOG_DEBUG(log, "The number of CPU is checked: {}, MAX: {}.", cpu_count, MAX_CPU_COMMUNITY_EDITION);
        else
        {
            LOG_ERROR(log, "The number of CPU exceeds the limit: {}, MAX: {}.", cpu_count, MAX_CPU_COMMUNITY_EDITION);
            throw DB::Exception(
                DB::ErrorCodes::LIMIT_EXCEEDED,
                "Check hardware resource limits of community edition failed, the number of CPU exceeds the limit.");
        }

        if (memory_amount <= MAX_MEMORY_COMMUNITY_EDITION)
            LOG_DEBUG(log, "Memory amount is checked: {}, MAX: {}.", memory_amount, MAX_MEMORY_COMMUNITY_EDITION);
        else
        {
            LOG_ERROR(log, "Memory amount exceeds the limit: {}, MAX: {}.", memory_amount, MAX_MEMORY_COMMUNITY_EDITION);
            throw DB::Exception(
                DB::ErrorCodes::LIMIT_EXCEEDED,
                "Check hardware resource limits of community edition failed, memory amount exceeds the limit.");
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException("checkHardwareResourceLimits");
        BaseDaemon::terminate();
    }
}

inline Poco::JSON::Object::Ptr convertStringToJson(const String & content)
{
    Poco::JSON::Parser parser;
    return parser.parse(content).extract<Poco::JSON::Object::Ptr>();
}

} // namespace MyscaleLicense
