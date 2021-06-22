#include <Parsers/ASTCreateConnectionQuery.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatRenameTo(const String & new_name, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " RENAME TO " << (settings.hilite ? IAST::hilite_none : "")
                      << quoteString(new_name);
    }

    void formatProvider(const Field & provider_name_value, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " PROVIDER = " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, provider_name_value);
    }

    void formatRoleARN(const Field & role_arn_value, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AWS_ROLE_ARN = " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, role_arn_value);
    }
 
    void formatExternalID(const Field & external_id_value, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AWS_ROLE_EXTERNAL_ID = " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, external_id_value);
    }
 
    void formatDuration(const Field & duration_value, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " AWS_ROLE_CREDENTIAL_DURATION = " << (settings.hilite ? IAST::hilite_none : "")
                      << applyVisitor(FieldVisitorToString{}, duration_value);
    }
}


String ASTCreateConnectionQuery::getID(char) const
{
    return "CreateConnectionQuery";
}


ASTPtr ASTCreateConnectionQuery::clone() const
{
    return std::make_shared<ASTCreateConnectionQuery>(*this);
}


void ASTCreateConnectionQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (attach)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << "ATTACH CONNECTION" << (format.hilite ? hilite_none : "");
    }
    else if (alter)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << "ALTER CONNECTION" << (format.hilite ? hilite_none : "");
    }
    else
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << "CREATE ";

        if (or_replace)
            format.ostr << " OR REPLACE ";

        format.ostr << "CONNECTION" << (format.hilite ? hilite_none : "");
    }
    if (if_exists)
        format.ostr << (format.hilite ? hilite_keyword : "") << " IF EXISTS" << (format.hilite ? hilite_none : "");
    else if (if_not_exists)
        format.ostr << (format.hilite ? hilite_keyword : "") << " IF NOT EXISTS" << (format.hilite ? hilite_none : "");

    format.ostr << " " << backQuoteIfNeed(name);
 
    formatOnCluster(format);

    if (!new_name.empty())
        formatRenameTo(new_name, format);

    if (!provider_name_value.isNull())
        formatProvider(provider_name_value, format);

    if (!role_arn_value.isNull())
        formatRoleARN(role_arn_value, format);

    if (!external_id_value.isNull())
        formatExternalID(external_id_value, format);

    if (!duration_value.isNull())
        formatDuration(duration_value, format);
}

}
