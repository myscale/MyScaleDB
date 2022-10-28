#include <VectorIndex/Metadata.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace VectorIndex
{
void Metadata::readText(DB::ReadBuffer & buf)
{
    DB::assertString("vector index metadata format version: 1\n", buf);
    DB::assertString("num_segments: 1\n", buf);

    DB::assertString("version: ", buf);
    DB::readString(version, buf);
    DB::assertChar('\n', buf);

    DB::assertString("type: ", buf);
    DB::String type_str;
    DB::readString(type_str, buf);
    Search::findEnumByName(type_str, type);
    DB::assertChar('\n', buf);

    DB::assertString("metric: ", buf);
    DB::String metric_str;
    DB::readString(metric_str, buf);
    Search::findEnumByName(metric_str, metric);
    DB::assertChar('\n', buf);

    DB::assertString("dimension: ", buf);
    DB::readIntText(dimension, buf);
    DB::assertChar('\n', buf);

    DB::assertString("total_vec: ", buf);
    DB::readIntText(total_vec, buf);
    DB::assertChar('\n', buf);

    DB::assertString("fallback_to_flat: ", buf);
    DB::readBoolText(fallback_to_flat, buf);
    DB::assertChar('\n', buf);

    DB::String current_part_name;
    DB::assertString("current_part_name: ", buf);
    DB::readString(current_part_name, buf);
    DB::assertChar('\n', buf);

    DB::String owner_part_name;
    DB::assertString("owner_part_name: ", buf);
    DB::readString(owner_part_name, buf);
    DB::assertChar('\n', buf);

    DB::String vector_index_name;
    DB::assertString("vector_index_name: ", buf);
    DB::readString(vector_index_name, buf);
    DB::assertChar('\n', buf);

    DB::String column_name;
    DB::assertString("column_name: ", buf);
    DB::readString(column_name, buf);
    DB::assertChar('\n', buf);

    UInt8 owner_part_id;
    DB::assertString("owner_part_id: ", buf);
    DB::readIntText(owner_part_id, buf);
    DB::assertChar('\n', buf);

    String key;
    String value;

    size_t num_params = 0;
    DB::assertString("num_params: ", buf);
    DB::readIntText(num_params, buf);
    DB::assertChar('\n', buf);

    for (size_t i = 0; i < num_params; i++)
    {
        readBackQuotedStringWithSQLStyle(key, buf);
        assertChar(' ', buf);
        readString(value, buf);
        assertChar('\n', buf);
        build_params.setParam(key, value);
    }

    size_t num_infos = 0;
    DB::assertString("num_infos: ", buf);
    DB::readIntText(num_infos, buf);
    DB::assertChar('\n', buf);

    for (size_t i = 0; i < num_infos; i++)
    {
        readBackQuotedStringWithSQLStyle(key, buf);
        assertChar(' ', buf);
        readString(value, buf);
        assertChar('\n', buf);
        infos[key] = value;
    }

    assertEOF(buf);
}

void Metadata::writeText(DB::WriteBuffer & buf) const
{
    DB::writeString("vector index metadata format version: 1\n", buf);
    DB::writeString("num_segments: 1\n", buf);

    DB::writeString("version: ", buf);
    DB::writeString(version, buf);
    DB::writeChar('\n', buf);

    DB::writeString("type: ", buf);
    DB::writeString(Search::enumToString(type), buf);
    DB::writeChar('\n', buf);

    DB::writeString("metric: ", buf);
    DB::writeString(Search::enumToString(metric), buf);
    DB::writeChar('\n', buf);

    DB::writeString("dimension: ", buf);
    DB::writeIntText(dimension, buf);
    DB::writeChar('\n', buf);

    DB::writeString("total_vec: ", buf);
    DB::writeIntText(total_vec, buf);
    DB::writeChar('\n', buf);

    DB::writeString("fallback_to_flat: ", buf);
    DB::writeBoolText(fallback_to_flat, buf);
    DB::writeChar('\n', buf);

    DB::writeString("current_part_name: ", buf);
    DB::writeString(segment_id.current_part_name, buf);
    DB::writeChar('\n', buf);

    DB::writeString("owner_part_name: ", buf);
    DB::writeString(segment_id.owner_part_name, buf);
    DB::writeChar('\n', buf);

    DB::writeString("vector_index_name: ", buf);
    DB::writeString(segment_id.vector_index_name, buf);
    DB::writeChar('\n', buf);

    DB::writeString("column_name: ", buf);
    DB::writeString(segment_id.column_name, buf);
    DB::writeChar('\n', buf);

    DB::writeString("owner_part_id: ", buf);
    DB::writeIntText(segment_id.owner_part_id, buf);
    DB::writeChar('\n', buf);

    DB::writeString("num_params: ", buf);
    DB::writeIntText(build_params.size(), buf);
    DB::writeChar('\n', buf);

    for (const auto & it : build_params)
    {
        DB::writeBackQuotedString(it.first, buf);
        DB::writeChar(' ', buf);
        DB::writeString(it.second, buf);
        DB::writeChar('\n', buf);
    }

    DB::writeString("num_infos: ", buf);
    DB::writeIntText(infos.size(), buf);
    DB::writeChar('\n', buf);

    for (const auto & it : infos)
    {
        DB::writeBackQuotedString(it.first, buf);
        DB::writeChar(' ', buf);
        DB::writeString(it.second, buf);
        DB::writeChar('\n', buf);
    }
}
}
