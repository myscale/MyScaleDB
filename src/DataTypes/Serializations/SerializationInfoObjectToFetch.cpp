#include <DataTypes/Serializations/SerializationInfoObjectToFetch.h>
#include <DataTypes/DataTypeObjectToFetch.h>
#include <Columns/ColumnObjectToFetch.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int THERE_IS_NO_COLUMN;
}

SerializationInfoObjectToFetch::SerializationInfoObjectToFetch(
    MutableSerializationInfos elems_, const Settings & settings_)
    : SerializationInfo(ISerialization::Kind::DEFAULT, settings_)
    , elems(std::move(elems_))
{
}

bool SerializationInfoObjectToFetch::hasCustomSerialization() const
{
    return std::any_of(elems.begin(), elems.end(), [](const auto & elem) { return elem->hasCustomSerialization(); });
}

void SerializationInfoObjectToFetch::add(const IColumn & column)
{
    SerializationInfo::add(column);

    const auto & column_objecttofetch = assert_cast<const ColumnObjectToFetch &>(column);
    const auto & right_elems = column_objecttofetch.getColumns();
    assert(elems.size() == right_elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->add(*right_elems[i]);
}

void SerializationInfoObjectToFetch::add(const SerializationInfo & other)
{
    SerializationInfo::add(other);

    const auto & info_objecttofetch = assert_cast<const SerializationInfoObjectToFetch &>(other);
    assert(elems.size() == info_objecttofetch.elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->add(*info_objecttofetch.elems[i]);
}

void SerializationInfoObjectToFetch::replaceData(const SerializationInfo & other)
{
    SerializationInfo::add(other);

    const auto & info_objecttofetch = assert_cast<const SerializationInfoObjectToFetch &>(other);
    assert(elems.size() == info_objecttofetch.elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->replaceData(*info_objecttofetch.elems[i]);
}

MutableSerializationInfoPtr SerializationInfoObjectToFetch::clone() const
{
    MutableSerializationInfos elems_cloned;
    elems_cloned.reserve(elems.size());
    for (const auto & elem : elems)
        elems_cloned.push_back(elem->clone());

    return std::make_shared<SerializationInfoObjectToFetch>(std::move(elems_cloned), settings);
}

void SerializationInfoObjectToFetch::serialializeKindBinary(WriteBuffer & out) const
{
    SerializationInfo::serialializeKindBinary(out);
    for (const auto & elem : elems)
        elem->serialializeKindBinary(out);
}

void SerializationInfoObjectToFetch::deserializeFromKindsBinary(ReadBuffer & in)
{
    SerializationInfo::deserializeFromKindsBinary(in);
    for (const auto & elem : elems)
        elem->deserializeFromKindsBinary(in);
}

Poco::JSON::Object SerializationInfoObjectToFetch::toJSON() const
{
    auto object = SerializationInfo::toJSON();
    Poco::JSON::Array subcolumns;
    for (const auto & elem : elems)
        subcolumns.add(elem->toJSON());

    object.set("subcolumns", subcolumns);
    return object;
}

void SerializationInfoObjectToFetch::fromJSON(const Poco::JSON::Object & object)
{
    SerializationInfo::fromJSON(object);

    if (!object.has("subcolumns"))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Missed field '{}' in SerializationInfo of columns SerializationInfoObjectToFetch");

    auto subcolumns = object.getArray("subcolumns");
    if (elems.size() != subcolumns->size())
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
            "Mismatched number of subcolumns between JSON and SerializationInfoObjectToFetch."
            "Expected: {}, got: {}", elems.size(), subcolumns->size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->fromJSON(*subcolumns->getObject(static_cast<uint32_t>(i)));
}

}
