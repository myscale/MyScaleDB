#include <Storages/VectorIndexInfo.h>
#include <VectorIndex/VectorSegmentExecutor.h>

namespace DB
{

VectorIndexInfo::VectorIndexInfo(
    const String & database_,
    const String & table_,
    const UInt64 & total_vec_,
    const VectorIndexDescription & desc,
    const VectorIndex::SegmentId & segment_id,
    const MergeTreeSettingsPtr & settings,
    const StorageInMemoryMetadata & metadata,
    const VectorIndexStatus & status_,
    const String & err_msg_)
    : database(database_)
    , table(table_)
    , part(segment_id.current_part_name)
    , owner_part(segment_id.current_part_name == segment_id.owner_part_name ? "" : segment_id.owner_part_name)
    , owner_part_id(segment_id.owner_part_id)
    , name(desc.name)
    , dimension(metadata.getConstraints().getArrayLengthByColumnName(desc.column).first)
    , total_vec(total_vec_)
    , status(status_)
    , err_msg(err_msg_)
{
    try
    {
        auto parameters = VectorIndex::convertPocoJsonToMap(desc.parameters);
        auto index_type = VectorIndex::getIndexType(desc.type);
        auto metric = VectorIndex::getMetric(parameters.extractParam("metric_type", std::string(settings->vector_search_metric_type)));

        VectorIndex::VectorSegmentExecutor executor(
            segment_id,
            index_type,
            metric,
            dimension,
            total_vec,
            parameters,
            settings->min_bytes_to_build_vector_index,
            settings->default_mstg_disk_mode);

        auto usage = executor.getIndexResourceUsage();
        memory_usage_bytes = usage.memory_usage_bytes;
        disk_usage_bytes = usage.disk_usage_bytes;

        /// actual type might fall back to FLAT if index size is smaller than min_bytes_to_build_vector_index
        type = Search::enumToString(executor.getIndexType());

        vector_memory_size_metric.changeTo(memory_usage_bytes);
    }
    catch (...)
    {
        status = VectorIndexStatus::ERROR;
        err_msg = getCurrentExceptionMessage(false);
    }
}

String VectorIndexInfo::statusString() const
{
    return Search::enumToString(status);
}

}
