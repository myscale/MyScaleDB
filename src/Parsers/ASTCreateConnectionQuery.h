#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Core/Field.h>


namespace DB
{

/** CREATE [OR REPLACE] CONNECTION [IF NOT EXISTS] connection_name
  *     PROVIDER = 'AWS' 
  *     AWS_ROLE_ARN = role_arn
  *     [AWS_ROLE_EXTERNAL_ID = external_id]
  *     [AWS_ROLE_CREDENTIAL_DURATION = duration]
  *
  * ALTER CONNECTION [IF EXISTS] connection_name
  *     [RENAME TO new_connection_name]
  *     [AWS_ROLE_ARN = value]
  *     [AWS_ROLE_EXTERNAL_ID = value]
  *     [AWS_ROLE_CREDENTIAL_DURATION = value]
  */
class ASTCreateConnectionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool alter = false;
    bool attach = false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;

    String name;
    String new_name;
    Field provider_name_value;
    Field role_arn_value;
    Field external_id_value;
    Field duration_value;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateConnectionQuery>(clone()); }
};
}
