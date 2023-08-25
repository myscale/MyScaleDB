#include <SearchIndex/Common/Utils.h>
#include <Storages/VectorIndexInfo.h>
#include <VectorIndex/Metadata.h>

namespace DB
{

VectorIndexInfo::VectorIndexInfo(
    const String & database_,
    const String & table_,
    const VectorIndex::Metadata & metadata,
    VectorIndexStatus status_,
    const String & err_msg_)
    : database(database_), table(table_), status(status_), err_msg(err_msg_)
{
    part = metadata.segment_id.current_part_name;
    owner_part = metadata.segment_id.owner_part_name;
    owner_part_id = metadata.segment_id.owner_part_id;
    name = metadata.segment_id.vector_index_name;
    type = Search::enumToString(metadata.type);
    total_vec = metadata.total_vec;

    setIndexSize(metadata);
}

String VectorIndexInfo::statusString() const
{
    return Search::enumToString(status);
}

String VectorIndexInfo::getMetadataInfoString(const String & info_name, const VectorIndex::Metadata & metadata)
{
    if (metadata.infos.contains(info_name))
        return metadata.infos.at(info_name);
    else
        return "";
}

}
