#include <fstream>
#include <sstream>

#include <Core/ServerUUID.h>
#include <base/getMemoryAmount.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/ShellCommand.h>
#include <Common/StringUtils.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Daemon/BaseDaemon.h>

#include "LicenseCheck.h"

namespace MyscaleLicense
{

ILicenseChecker::ILicenseChecker(
    const Poco::Util::LayeredConfiguration & server_config_,
    const ContextMutablePtr global_context,
    const XMLDocumentPtr & preprocessed_xml)
    : WithMutableContext(global_context)
    , server_config(server_config_)
    , kubeconfig_enabled(global_context->getConfigRef().getBool("kubernetes_enabled", false))
    , machine_id_digest(getMachineIDDigest())
    , system_uuid_digest(kubeconfig_enabled ? "" : getSystemUUIDDigest())
    , server_uuid_digest(getHexDigest(toString(DB::ServerUUID::get())))
    , cpu_count(getNumberOfPhysicalCPUCores())
    , memory_amount(getMemoryAmount())
    , public_key_path(getLicenseFilePathPrefix(global_context->getConfigRef()) + RSA_PUBLIC_KEY_FILE_NAME)
    , license_doc(getLicenseDoc(preprocessed_xml, true))
    , retry_times(RETRY_TIMES)
{
}

void ILicenseChecker::CheckLicenseTask()
{
    try
    {
        LicenseCheckCtxPtr check_ctx;
        {
            std::lock_guard<std::mutex> lock(license_mutex);
            check_ctx = prepareCheckLicenseCtx(license_doc);
        }
        checkLicenseImpl(*check_ctx);

        size_t check_period = server_config.getUInt64("license_check_period", MAX_CHECK_PERIOD);
        /// limit max check_period with 1 day
        if (check_period > MAX_CHECK_PERIOD)
            check_period = MAX_CHECK_PERIOD;
        LOG_DEBUG(log, "Schedule to check license again in {} seconds.", check_period);
        (*license_task)->scheduleAfter(check_period * 1000);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::LICENSE_ERROR || retry_times <= 0)
        {
            LOG_ERROR(log, "An error occurred while checking license, please check the relevant configuration. error: {}", e.what());
            BaseDaemon::terminate();
        }
        else
        {
            tryLogCurrentException("checkLicense");
            --retry_times;
            (*license_task)->scheduleAfter(RETRY_INTERVAL_S * 1000);
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "An error occurred while checking license, please check the relevant configuration.");
        tryLogCurrentException("checkLicense");
        BaseDaemon::terminate();
    }
}

void ILicenseChecker::reloadLicenseInfo(const XMLDocumentPtr & preprocessed_xml)
{
    auto reload_license_doc = getLicenseDoc(preprocessed_xml, true);
    if (checkReloadLicenseModifyValid(license_doc, reload_license_doc))
    {
        LOG_DEBUG(log, "Reload license info.");
        try
        {
            auto check_ctx = prepareCheckLicenseCtx(reload_license_doc, true);
            checkLicenseImpl(*check_ctx);
        }
        catch (DB::Exception & e)
        {
            LOG_ERROR(
                log, "An error occurred while checking license, please check the relevant configuration. error {}: {}", e.code(), e.what());
            return;
        }
        catch (...)
        {
            LOG_ERROR(log, "An error occurred while checking license, please check the relevant configuration.");
            return;
        }

        /// check ok, update license doc
        {
            std::lock_guard<std::mutex> lock(license_mutex);
            license_doc = reload_license_doc;
        }
    }
}

String ILicenseChecker::getLicenseFilePathPrefix(const Poco::Util::AbstractConfiguration & config) const
{
    String license_file_path_prefix = config.getString("license_file_path", "/etc/clickhouse-server");
    trim(license_file_path_prefix);
    if (!license_file_path_prefix.ends_with('/'))
        license_file_path_prefix += "/";
    return license_file_path_prefix;
}

String ILicenseChecker::getLicenseFileContent(const String & path) const
{
    std::ifstream license_file(path);
    std::stringstream license_buffer;
    license_buffer << license_file.rdbuf();
    String license_file_content(license_buffer.str());
    trimRight(license_file_content, '\n');
    license_file.close();
    return license_file_content;
}

void ILicenseChecker::checkLicenseSign(const LicenseCheckCtx & check_ctx)
{
    auto * license_sign_node = check_ctx.license_doc->getNodeByPath(LICENSE_SIGN_PATH_XML);
    if (license_sign_node)
    {
        String license_info;
        auto * license_info_node = check_ctx.license_doc->getNodeByPath(LICENSE_INFO_PATH_XML);
        if (license_info_node)
        {
            XMLDocumentPtr license_info_doc = new Poco::XML::Document;
            auto * copy_node = license_info_doc->importNode(license_info_node, true);
            license_info_doc->appendChild(copy_node);

            std::stringstream s; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            Poco::XML::DOMWriter writer;
            writer.writeNode(s, license_info_doc);
            license_info = s.str();
        }
        else
        {
            LOG_ERROR(log, "Get license info failed, please check the XML structure of license.");
            throw Exception(ErrorCodes::LICENSE_ERROR, "Get license info failed, the XML structure of license is incorrect.");
        }

        String license_sign = license_sign_node->innerText();
        String decoded_sign = base64Decode(license_sign);

#if USE_SSL
        RSA * rsa_public_key = nullptr;
        auto * license_public_key_node = check_ctx.license_doc->getNodeByPath(LICENSE_PUBLIC_KEY_PATH_XML);
        if (license_public_key_node)
        {
            String public_key = license_public_key_node->innerText();
            String decoded_public_key = base64Decode(public_key);
            BIO * bio_public_key = BIO_new_mem_buf(decoded_public_key.c_str(), -1);
            rsa_public_key = PEM_read_bio_RSAPublicKey(bio_public_key, nullptr, nullptr, nullptr);
            BIO_free(bio_public_key);
        }
        else
        {
            BIO * bio_public_key = BIO_new(BIO_s_file());
            BIO_read_filename(bio_public_key, const_cast<char*>(public_key_path.c_str()));
            rsa_public_key = PEM_read_bio_RSA_PUBKEY(bio_public_key, nullptr, nullptr, nullptr);
            BIO_free(bio_public_key);
        }
        if (rsa_public_key == nullptr)
        {
            unsigned long err_code = ERR_get_error();
            char err_buf[256];
            ERR_error_string_n(err_code, err_buf, sizeof(err_buf));
            throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, read license public key failed. {}", err_buf);
        }
        uint8_t digest[33];
        SHA256(reinterpret_cast<const uint8_t *>(license_info.c_str()), license_info.size(), digest);
        int result = RSA_verify(
            NID_sha256, digest, 32, reinterpret_cast<const uint8_t *>(decoded_sign.c_str()), static_cast<unsigned int>(decoded_sign.size()), rsa_public_key);
        RSA_free(rsa_public_key);
        if (result != 1)
        {
            LOG_ERROR(log, "Check license signature failed, license has been modified.");
            throw Exception(ErrorCodes::LICENSE_ERROR, "Check license signature failed.");
        }
#else
        LOG_ERROR(log, "Check license signature failed, OpenSSL is not enabled.");
        throw Exception(ErrorCodes::LICENSE_ERROR, "Check license signature failed, OpenSSL is not enabled.");
#endif
    }
    else
    {
        LOG_ERROR(log, "Get license signature failed, please check the XML structure of license.");
        throw Exception(ErrorCodes::LICENSE_ERROR, "Get license signature failed, the XML structure of license is incorrect.");
    }
}

void ILicenseChecker::checkMachineResource(const LicenseCheckCtx & check_ctx)
{
    auto * cpu_count_node = check_ctx.license_doc->getNodeByPath(CPU_COUNT_PATH_XML);
    if (cpu_count_node)
    {
        String cpu_count_xml = cpu_count_node->innerText();
        if (cpu_count <= std::stoul(cpu_count_xml))
            LOG_DEBUG(log, "The number of CPU is checked: {}, MAX: {}.", cpu_count, cpu_count_xml);
        else
        {
            LOG_ERROR(log, "The number of CPU exceeds the limit: {}, MAX: {}.", cpu_count, cpu_count_xml);
            throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, the number of CPU exceeds the limit.");
        }
    }
    else
    {
        LOG_ERROR(log, "Get the number of CPU failed, please check the XML structure of license.");
        throw Exception(ErrorCodes::LICENSE_ERROR, "Get the number of CPU failed, the XML structure of license is incorrect.");
    }

    auto * memory_amount_node = check_ctx.license_doc->getNodeByPath(MEMORY_AMOUNT_PATH_XML);
    if (memory_amount_node)
    {
        String memory_amount_xml = memory_amount_node->innerText();
        if (memory_amount <= std::strtoull(memory_amount_xml.c_str(), nullptr, 10))
            LOG_DEBUG(log, "Memory amount is checked: {}, MAX: {}.", memory_amount, memory_amount_xml);
        else
        {
            LOG_ERROR(log, "Memory amount exceeds the limit: {}, MAX: {}.", memory_amount, memory_amount_xml);
            throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, memory amount exceeds the limit.");
        }
    }
    else
    {
        LOG_ERROR(log, "Get memory amount failed, please check the XML structure of license.");
        throw Exception(ErrorCodes::LICENSE_ERROR, "Get memory amount failed, the XML structure of license is incorrect.");
    }
}

void ILicenseChecker::checkExpiration(const LicenseCheckCtx & check_ctx)
{
    String expiration_str = check_ctx.license_doc->getNodeByPath(EXPIRATION_PATH_XML)->innerText();
    tm tm_struct;
    sscanf(
        expiration_str.c_str(),
        "%d-%d-%d %d:%d:%d",
        &tm_struct.tm_year,
        &tm_struct.tm_mon,
        &tm_struct.tm_mday,
        &tm_struct.tm_hour,
        &tm_struct.tm_min,
        &tm_struct.tm_sec);
    tm_struct.tm_year -= 1900;
    tm_struct.tm_mon--;
    tm_struct.tm_isdst = -1;
    auto expiration = mktime(&tm_struct);
    auto now = time(nullptr);
    if (expiration > now)
    {
        LOG_DEBUG(log, "License is valid, expiration date: {}.", expiration_str);
    }
    else
    {
        LOG_ERROR(log, "License has expired, expiration date: {}.", expiration_str);
        throw Exception(ErrorCodes::LICENSE_ERROR, "License has expired.");
    }
}

void ILicenseChecker::checkMachineInfo(const LicenseCheckCtx & check_ctx)
{
    if (kubeconfig_enabled)
    {
        LOG_WARNING(log, "Kubeconfig is enabled, skip machine info check.");
        return;
    }

    Poco::XML::NodeList* nodes = check_ctx.license_doc->getElementsByTagName(NODE_INFO_TAG);
    for (unsigned long i = 0; i < nodes->length(); ++i)
    {
        Poco::XML::Element* node_info_elem = dynamic_cast<Poco::XML::Element*>(nodes->item(i));
        if (node_info_elem && node_info_elem->getAttribute(MACHINE_ID_TAG) == machine_id_digest)
        {
            LOG_DEBUG(log, "Given id is checked, given id: {}.", machine_id_digest);

            String system_uuid_xml = node_info_elem->getChildElement(SYSTEM_UUID_TAG)->innerText();
            if (system_uuid_digest == system_uuid_xml)
            {
                LOG_DEBUG(log, "Machine info is matched, machine info: {}.", system_uuid_digest);
                return;
            }
        }
        continue;
    }
    /// no registered machine, throw exception, licensecheck failed
    LOG_ERROR(log, "There is no such machine in license, some info may be modified, current given id: {}.", machine_id_digest);
    throw Exception(ErrorCodes::LICENSE_ERROR, "Check license failed, there is no such machine.");
}

XMLDocumentPtr ILicenseChecker::getFormatLicenseInfoDoc(XMLDocumentPtr license_doc_, bool new_format_license) const
{
    LOG_DEBUG(log, "Get formatted license document");
    /// Parse data
    String cluster_name_in_doc, instance_count_in_doc, cpu_count_in_doc, memory_amount_in_doc, expiration, license_sign, license_public_key;
    std::vector<std::pair<String, String>> machines_info;
    {
        Node * cluster_name_node = license_doc_->getNodeByPath(CLUSTER_NAME_PATH_XML);
        if (cluster_name_node)
        {
            cluster_name_in_doc = cluster_name_node->innerText();
            trim(cluster_name_in_doc);
        }
        Node * instance_count_node = license_doc_->getNodeByPath(INSTANCE_COUNT_PATH_XML);
        if (instance_count_node)
        {
            instance_count_in_doc = instance_count_node->innerText();
            trim(instance_count_in_doc);
        }
        Node * cpu_count_node = license_doc_->getNodeByPath(CPU_COUNT_PATH_XML);
        if (cpu_count_node)
        {
            cpu_count_in_doc = cpu_count_node->innerText();
            trim(cpu_count_in_doc);
        }
        Node * memory_amount_node = license_doc_->getNodeByPath(MEMORY_AMOUNT_PATH_XML);
        if (memory_amount_node)
        {
            memory_amount_in_doc = memory_amount_node->innerText();
            trim(memory_amount_in_doc);
        }
        Node * expiration_node = license_doc_->getNodeByPath(EXPIRATION_PATH_XML);
        if (expiration_node)
        {
            expiration = expiration_node->innerText();
            trim(expiration);
        }

        Node * nodes_info = license_doc_->getNodeByPath(NODES_INFO_PATH_XML);
        if (nodes_info)
        {
            Poco::XML::NodeList * nodes = nodes_info->childNodes();
            Node * next_node_info = nullptr;
            for (Node * node_info = nodes->item(0); node_info; node_info = next_node_info)
            {
                next_node_info = node_info->nextSibling();
                if (node_info->nodeType() == Node::ELEMENT_NODE)
                {
                    Element * elem = dynamic_cast<Element *>(node_info);
                    if (elem)
                    {
                        String machine_id = elem->getAttribute(MACHINE_ID_TAG);
                        trim(machine_id);

                        String system_uuid;
                        Element * system_uuid_node = elem->getChildElement(SYSTEM_UUID_TAG);
                        if (system_uuid_node)
                        {
                            system_uuid = system_uuid_node->innerText();
                            trim(system_uuid);
                        }

                        machines_info.push_back(std::make_pair(machine_id, system_uuid));
                    }
                }
            }
        }

        Node * license_sign_node = license_doc_->getNodeByPath(LICENSE_SIGN_PATH_XML);
        if (license_sign_node)
        {
            license_sign = license_sign_node->innerText();
            trim(license_sign);
        }
        Node * license_public_key_node = license_doc_->getNodeByPath(LICENSE_PUBLIC_KEY_PATH_XML);
        if (license_public_key_node)
        {
            license_public_key = license_public_key_node->innerText();
            trim(license_public_key);
        }
    }

    if (cluster_name_in_doc.empty() || instance_count_in_doc.empty() || cpu_count_in_doc.empty() || memory_amount_in_doc.empty()
        || expiration.empty() || license_sign.empty())
    {
        LOG_ERROR(log, "Format license failed, please check the XML structure of license.");
        throw Exception(ErrorCodes::LICENSE_ERROR, "Format license failed, the XML structure of license may be incorrect.");
    }

    /// Format XML document
    XMLDocumentPtr rebuild_doc = new Poco::XML::Document;
    ElementPtr license_node(rebuild_doc->createElement(LICENSE_TAG));

    if (new_format_license)
    {
        ElementPtr clickhouse_node(rebuild_doc->createElement("clickhouse"));
        rebuild_doc->appendChild(clickhouse_node);
        clickhouse_node->appendChild(license_node);
    }
    else
        rebuild_doc->appendChild(license_node);

    ElementPtr license_info_node(rebuild_doc->createElement(LICENSE_INFO_TAG));
    license_node->appendChild(license_info_node);

    ElementPtr cluster_name_node(rebuild_doc->createElement(CLUSTER_NAME_TAG));
    license_info_node->appendChild(cluster_name_node);
    TextPtr cluster_name_text(rebuild_doc->createTextNode(cluster_name_in_doc));
    cluster_name_node->appendChild(cluster_name_text);

    ElementPtr instance_count_node(rebuild_doc->createElement(INSTANCE_COUNT_TAG));
    license_info_node->appendChild(instance_count_node);
    TextPtr instance_count_text(rebuild_doc->createTextNode(instance_count_in_doc));
    instance_count_node->appendChild(instance_count_text);

    ElementPtr cpu_count_node(rebuild_doc->createElement(INSTANCE_CPU_TAG));
    license_info_node->appendChild(cpu_count_node);
    TextPtr cpu_count_text(rebuild_doc->createTextNode(cpu_count_in_doc));
    cpu_count_node->appendChild(cpu_count_text);

    ElementPtr memory_amount_node(rebuild_doc->createElement(INSTANCE_MEMORY_TAG));
    license_info_node->appendChild(memory_amount_node);
    TextPtr memory_amount_text(rebuild_doc->createTextNode(memory_amount_in_doc));
    memory_amount_node->appendChild(memory_amount_text);

    if (!machines_info.empty())
    {
        ElementPtr nodes_info(rebuild_doc->createElement(NODES_INFO_TAG));
        license_info_node->appendChild(nodes_info);
        for (auto & machine_info : machines_info)
        {
            ElementPtr node_info(rebuild_doc->createElement(NODE_INFO_TAG));
            node_info->setAttribute(MACHINE_ID_TAG, machine_info.first);
            nodes_info->appendChild(node_info);

            ElementPtr system_uuid(rebuild_doc->createElement(SYSTEM_UUID_TAG));
            node_info->appendChild(system_uuid);
            TextPtr system_uuid_text(rebuild_doc->createTextNode(machine_info.second));
            system_uuid->appendChild(system_uuid_text);
        }
    }

    ElementPtr expiration_node(rebuild_doc->createElement(EXPIRED_DATE_TAG));
    license_info_node->appendChild(expiration_node);
    TextPtr expiration_text(rebuild_doc->createTextNode(expiration));
    expiration_node->appendChild(expiration_text);

    ElementPtr signature_node(rebuild_doc->createElement(LICENSE_SIGNATURE_TAG));
    license_node->appendChild(signature_node);
    TextPtr signature_text(rebuild_doc->createTextNode(license_sign));
    signature_node->appendChild(signature_text);

    if (!license_public_key.empty())
    {
        ElementPtr public_key_node(rebuild_doc->createElement(LICENSE_PUBLIC_KEY_TAG));
        license_node->appendChild(public_key_node);
        TextPtr public_key_text(rebuild_doc->createTextNode(license_public_key));
        public_key_node->appendChild(public_key_text);
    }

    /// Compatible with license of old format
    std::stringstream s; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::DOMWriter writer;
    writer.setNewLine("\n");
    writer.setIndent("    ");
    writer.setOptions(Poco::XML::XMLWriter::PRETTY_PRINT);
    writer.writeNode(s, rebuild_doc);
    String format_license = s.str();

    Poco::XML::DOMParser parser;
    XMLDocumentPtr format_doc = parser.parseString(format_license);

    if (new_format_license)
    {
        auto * new_format_license_doc = format_doc->getNodeByPath("/clickhouse" + LICENSE_PATH_XML);

        XMLDocumentPtr res = new Poco::XML::Document;
        auto * copy_node = res->importNode(new_format_license_doc, true);
        res->appendChild(copy_node);

        return res;
    }

    return format_doc;
}

XMLDocumentPtr ILicenseChecker::getLicenseDoc(const XMLDocumentPtr & preprocessed_xml, bool need_format) const
{
    /// License of New format
    /// Read from configuration in memory
    if (preprocessed_xml)
    {
        auto * license_node = preprocessed_xml->getNodeByPath("/clickhouse" + LICENSE_PATH_XML);
        if (license_node)
        {
            LOG_DEBUG(log, "License of New format");

            XMLDocumentPtr license_doc_ = new Poco::XML::Document;
            auto * copy_node = license_doc_->importNode(license_node, true);
            license_doc_->appendChild(copy_node);

            if (need_format)
                return getFormatLicenseInfoDoc(license_doc_, true);

            return license_doc_;
        }
    }

    /// Compatible with license of old format
    /// Read from license file
    LOG_DEBUG(log, "License of old format");

    String license_file_path = getLicenseFilePathPrefix(server_config) + LICENSE_FILE_NAME;
    String license_content = getLicenseFileContent(license_file_path);
    if (license_content.empty())
    {
        LOG_ERROR(log, "License is empty, server will be terminated");
        throw Exception(ErrorCodes::LICENSE_ERROR, "Empty license");
    }
    Poco::XML::DOMParser parser;
    XMLDocumentPtr license_doc_ = parser.parseString(license_content);
    if (need_format)
        return getFormatLicenseInfoDoc(license_doc_, false);

    return license_doc_;
}

bool ILicenseChecker::checkReloadLicenseModifyValid(const XMLDocumentPtr & current_license_doc, const XMLDocumentPtr & reload_license_doc)
{
    if (!current_license_doc || !reload_license_doc)
        return false;
    if (current_license_doc->getNodeByPath(CLUSTER_NAME_PATH_XML)->innerText()
        != reload_license_doc->getNodeByPath(CLUSTER_NAME_PATH_XML)->innerText())
    {
        LOG_ERROR(log, "Cluster name is different, reload license info fail.");
        return false;
    }
    if (current_license_doc->getNodeByPath(INSTANCE_COUNT_PATH_XML)->innerText()
        != reload_license_doc->getNodeByPath(INSTANCE_COUNT_PATH_XML)->innerText())
    {
        LOG_ERROR(log, "Instance count is different, reload license info fail.");
        return false;
    }
    if (current_license_doc->getNodeByPath(CPU_COUNT_PATH_XML)->innerText()
        != reload_license_doc->getNodeByPath(CPU_COUNT_PATH_XML)->innerText())
    {
        LOG_ERROR(log, "Cpu count is different, reload license info fail.");
        return false;
    }
    if (current_license_doc->getNodeByPath(MEMORY_AMOUNT_PATH_XML)->innerText()
        != reload_license_doc->getNodeByPath(MEMORY_AMOUNT_PATH_XML)->innerText())
    {
        LOG_ERROR(log, "Memory amount is different, reload license info fail.");
        return false;
    }
    return true;
}

}
