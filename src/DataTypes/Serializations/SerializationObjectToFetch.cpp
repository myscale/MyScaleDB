#include <DataTypes/Serializations/SerializationObjectToFetch.h>
#include <DataTypes/Serializations/SerializationInfoObjectToFetch.h>
#include <DataTypes/DataTypeObjectToFetch.h>
#include <Core/Field.h>
#include <Columns/ColumnObjectToFetch.h>
#include <Common/assert_cast.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}


static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return assert_cast<ColumnObjectToFetch &>(column).getColumn(idx);
}

static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return assert_cast<const ColumnObjectToFetch &>(column).getColumn(idx);
}

void SerializationObjectToFetch::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & objecttofetch = field.get<const ObjectToFetch &>();
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(objecttofetch[element_index], ostr, settings);
    }
}

void SerializationObjectToFetch::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    const size_t size = elems.size();

    field = ObjectToFetch();
    ObjectToFetch & tuple = field.get<ObjectToFetch &>();
    tuple.reserve(size);
    for (size_t i = 0; i < size; ++i)
        elems[i]->deserializeBinary(tuple.emplace_back(), istr, settings);
}

void SerializationObjectToFetch::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(extractElementColumn(column, element_index), row_num, ostr, settings);
    }
}


template <typename F>
static void addElementSafe(size_t num_elems, IColumn & column, F && impl)
{
    size_t old_size = column.size();

    try
    {
        impl();

        size_t new_size = column.size();
        for (size_t i = 1; i < num_elems; ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (element_column.size() != new_size)
                throw Exception(ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH,
                    "Cannot read a ObjectToFetch because not all elements are present");
        }
    }
    catch (...)
    {
        for (size_t i = 0; i < num_elems; ++i)
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
                element_column.popBack(1);
        }

        throw;
    }
}

void SerializationObjectToFetch::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe(elems.size(), column, [&]
    {
        for (size_t i = 0; i < elems.size(); ++i)
            elems[i]->deserializeBinary(extractElementColumn(column, i), istr, settings);
    });
}

void SerializationObjectToFetch::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('(', ostr);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextQuoted(extractElementColumn(column, i), row_num, ostr, settings);
    }
    writeChar(')', ostr);
}

void SerializationObjectToFetch::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    const size_t size = elems.size();
    assertChar('(', istr);

    addElementSafe(elems.size(), column, [&]
    {
        for (size_t i = 0; i < size; ++i)
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
            {
                assertChar(',', istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeTextQuoted(extractElementColumn(column, i), istr, settings);
        }
    });

    if (1 == elems.size())
    {
        skipWhitespaceIfAny(istr);
        checkChar(',', istr);
    }
    skipWhitespaceIfAny(istr);
    assertChar(')', istr);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "ObjectToFetch");
}

void SerializationObjectToFetch::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.json.write_named_tuples_as_objects
        && have_explicit_names)
    {
        writeChar('{', ostr);
        for (size_t i = 0; i < elems.size(); ++i)
        {
            if (i != 0)
            {
                writeChar(',', ostr);
            }
            writeJSONString(elems[i]->getElementName(), ostr, settings);
            writeChar(':', ostr);
            elems[i]->serializeTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
        }
        writeChar('}', ostr);
    }
    else
    {
        writeChar('[', ostr);
        for (size_t i = 0; i < elems.size(); ++i)
        {
            if (i != 0)
                writeChar(',', ostr);
            elems[i]->serializeTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
        }
        writeChar(']', ostr);
    }
}

void SerializationObjectToFetch::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.json.read_named_tuples_as_objects
        && have_explicit_names)
    {
        skipWhitespaceIfAny(istr);
        assertChar('{', istr);
        skipWhitespaceIfAny(istr);

        addElementSafe(elems.size(), column, [&]
        {
            // Require all elements but in arbitrary order.
            for (size_t i = 0; i < elems.size(); ++i)
            {
                if (i > 0)
                {
                    skipWhitespaceIfAny(istr);
                    assertChar(',', istr);
                    skipWhitespaceIfAny(istr);
                }

                std::string name;
                readDoubleQuotedString(name, istr);
                skipWhitespaceIfAny(istr);
                assertChar(':', istr);
                skipWhitespaceIfAny(istr);

                const size_t element_pos = getPositionByName(name);
                auto & element_column = extractElementColumn(column, element_pos);
                elems[element_pos]->deserializeTextJSON(element_column, istr, settings);
            }
        });

        skipWhitespaceIfAny(istr);
        assertChar('}', istr);
    }
    else
    {
        const size_t size = elems.size();
        assertChar('[', istr);

        addElementSafe(elems.size(), column, [&]
        {
            for (size_t i = 0; i < size; ++i)
            {
                skipWhitespaceIfAny(istr);
                if (i != 0)
                {
                    assertChar(',', istr);
                    skipWhitespaceIfAny(istr);
                }
                elems[i]->deserializeTextJSON(extractElementColumn(column, i), istr, settings);
            }
        });

        skipWhitespaceIfAny(istr);
        assertChar(']', istr);
    }
}

void SerializationObjectToFetch::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCString("<objecttofetch>", ostr);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        writeCString("<elem>", ostr);
        elems[i]->serializeTextXML(extractElementColumn(column, i), row_num, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</objecttofetch>", ostr);
}

void SerializationObjectToFetch::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextCSV(extractElementColumn(column, i), row_num, ostr, settings);
    }
}

void SerializationObjectToFetch::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe(elems.size(), column, [&]
    {
        const size_t size = elems.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (i != 0)
            {
                skipWhitespaceIfAny(istr);
                assertChar(settings.csv.delimiter, istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeTextCSV(extractElementColumn(column, i), istr, settings);
        }
    });
}

void SerializationObjectToFetch::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_tuple = data.type ? &assert_cast<const DataTypeObjectToFetch &>(*data.type) : nullptr;
    const auto * column_tuple = data.column ? &assert_cast<const ColumnObjectToFetch &>(*data.column) : nullptr;
    const auto * info_tuple = data.serialization_info ? &assert_cast<const SerializationInfoObjectToFetch &>(*data.serialization_info) : nullptr;

    for (size_t i = 0; i < elems.size(); ++i)
    {
        auto next_data = SubstreamData(elems[i])
            .withType(type_tuple ? type_tuple->getElement(i) : nullptr)
            .withColumn(column_tuple ? column_tuple->getColumnPtr(i) : nullptr)
            .withSerializationInfo(info_tuple ? info_tuple->getElementInfo(i) : nullptr);

        elems[i]->enumerateStreams(settings, callback, next_data);
    }
}

struct SerializeBinaryBulkStateObjectToFetch : public ISerialization::SerializeBinaryBulkState
{
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeBinaryBulkStateObjectToFetch : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;
};


void SerializationObjectToFetch::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto objecttofetch_state = std::make_shared<SerializeBinaryBulkStateObjectToFetch>();
    objecttofetch_state->states.resize(elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->serializeBinaryBulkStatePrefix(extractElementColumn(column, i), settings, objecttofetch_state->states[i]);

    state = std::move(objecttofetch_state);
}

void SerializationObjectToFetch::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetState<SerializeBinaryBulkStateObjectToFetch>(state);

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->serializeBinaryBulkStateSuffix(settings, tuple_state->states[i]);
}

void SerializationObjectToFetch::deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const
{
    auto objecttofetch_state = std::make_shared<DeserializeBinaryBulkStateObjectToFetch>();
    objecttofetch_state->states.resize(elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->deserializeBinaryBulkStatePrefix(settings, objecttofetch_state->states[i]);

    state = std::move(objecttofetch_state);
}

void SerializationObjectToFetch::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetState<SerializeBinaryBulkStateObjectToFetch>(state);

    for (size_t i = 0; i < elems.size(); ++i)
    {
        const auto & element_col = extractElementColumn(column, i);
        elems[i]->serializeBinaryBulkWithMultipleStreams(element_col, offset, limit, settings, tuple_state->states[i]);
    }
}

void SerializationObjectToFetch::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
     auto * tuple_state = checkAndGetState<DeserializeBinaryBulkStateObjectToFetch>(state);

    auto mutable_column = column->assumeMutable();
    auto & column_tuple = assert_cast<ColumnObjectToFetch &>(*mutable_column);

    settings.avg_value_size_hint = 0;
    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->deserializeBinaryBulkWithMultipleStreams(column_tuple.getColumnPtr(i), limit, settings, tuple_state->states[i], cache);
}

size_t SerializationObjectToFetch::getPositionByName(const String & name) const
{
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i)
        if (elems[i]->getElementName() == name)
            return i;
    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "ObjectToFetch doesn't have element with name '{}'", name);
}

}
