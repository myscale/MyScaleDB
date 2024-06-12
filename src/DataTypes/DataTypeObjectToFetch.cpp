#include <base/map.h>
#include <base/range.h>
#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnObjectToFetch.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeObjectToFetch.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/Serializations/SerializationObjectToFetch.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationInfoObjectToFetch.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DUPLICATE_COLUMN;
    extern const int EMPTY_DATA_PASSED;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
    extern const int ILLEGAL_INDEX;
    extern const int LOGICAL_ERROR;
}


DataTypeObjectToFetch::DataTypeObjectToFetch(const DataTypes & elems_)
    : elems(elems_), have_explicit_names(false)
{
    /// Automatically assigned names in form of '1', '2', ...
    size_t size = elems.size();
    names.resize(size);
    for (size_t i = 0; i < size; ++i)
        names[i] = toString(i + 1);
}

static std::optional<Exception> checkObjectToFetchNames(const Strings & names)
{
    std::unordered_set<String> names_set;
    for (const auto & name : names)
    {
        if (name.empty())
            return Exception(ErrorCodes::BAD_ARGUMENTS, "Names of ObjectToFetch elements cannot be empty");

        if (isNumericASCII(name[0]))
            return Exception(ErrorCodes::BAD_ARGUMENTS, "Explicitly specified names of ObjectToFetch elements cannot start with digit");

        if (!names_set.insert(name).second)
            return Exception(ErrorCodes::DUPLICATE_COLUMN, "Names of ObjectToFetch elements must be unique");
    }

    return {};
}

DataTypeObjectToFetch::DataTypeObjectToFetch(const DataTypes & elems_, const Strings & names_, bool serialize_names_)
    : elems(elems_), names(names_), have_explicit_names(true), serialize_names(serialize_names_)
{
    size_t size = elems.size();
    if (names.size() != size)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Wrong number of names passed to constructor of DataTypeObjectToFetch");

    if (auto exception = checkObjectToFetchNames(names))
        throw std::move(*exception);
}

std::string DataTypeObjectToFetch::doGetName() const
{
    size_t size = elems.size();
    WriteBufferFromOwnString s;

    s << "ObjectToFetch(";
    for (size_t i = 0; i < size; ++i)
    {
        if (i != 0)
            s << ", ";

        if (have_explicit_names && serialize_names)
            s << backQuoteIfNeed(names[i]) << ' ';

        s << elems[i]->getName();
    }
    s << ")";

    return s.str();
}


static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return assert_cast<ColumnObjectToFetch &>(column).getColumn(idx);
}

template <typename F>
static void addElementSafe(const DataTypes & elems, IColumn & column, F && impl)
{
    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    try
    {
        impl();

        // Check that all columns now have the same size.
        size_t new_size = column.size();

        for (auto i : collections::range(0, elems.size()))
        {
            const auto & element_column = extractElementColumn(column, i);
            if (element_column.size() != new_size)
            {
                // This is not a logical error because it may work with
                // user-supplied data.
                throw Exception(ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH,
                    "Cannot read a ObjectToFetch because not all elements are present");
            }
        }
    }
    catch (...)
    {
        for (const auto & i : collections::range(0, elems.size()))
        {
            auto & element_column = extractElementColumn(column, i);

            if (element_column.size() > old_size)
                element_column.popBack(1);
        }

        throw;
    }
}

MutableColumnPtr DataTypeObjectToFetch::createColumn() const
{
    size_t size = elems.size();
    MutableColumns objecttofetch_columns(size);
    for (size_t i = 0; i < size; ++i)
        objecttofetch_columns[i] = elems[i]->createColumn();
    return ColumnObjectToFetch::create(std::move(objecttofetch_columns));
}

MutableColumnPtr DataTypeObjectToFetch::createColumn(const ISerialization & serialization) const
{
    /// If we read subcolumn of nested Tuple, it may be wrapped to SerializationNamed
    /// several times to allow to reconstruct the substream path name.
    /// Here we don't need substream path name, so we drop first several wrapper serializations.

    const auto * current_serialization = &serialization;
    while (const auto * serialization_named = typeid_cast<const SerializationNamed *>(current_serialization))
        current_serialization = serialization_named->getNested().get();

    const auto * serialization_tuple = typeid_cast<const SerializationObjectToFetch *>(current_serialization);
    if (!serialization_tuple)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected serialization to create column of type ObjectToFetch");

    const auto & element_serializations = serialization_tuple->getElementsSerializations();

    size_t size = elems.size();
    assert(element_serializations.size() == size);
    MutableColumns tuple_columns(size);
    for (size_t i = 0; i < size; ++i)
        tuple_columns[i] = elems[i]->createColumn(*element_serializations[i]->getNested());

    return ColumnObjectToFetch::create(std::move(tuple_columns));
}

Field DataTypeObjectToFetch::getDefault() const
{
    return ObjectToFetch(collections::map<ObjectToFetch>(elems, [] (const DataTypePtr & elem) { return elem->getDefault(); }));
}

void DataTypeObjectToFetch::insertDefaultInto(IColumn & column) const
{
    addElementSafe(elems, column, [&]
    {
        for (const auto & i : collections::range(0, elems.size()))
            elems[i]->insertDefaultInto(extractElementColumn(column, i));
    });
}

bool DataTypeObjectToFetch::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeObjectToFetch & rhs_objecttofetch = static_cast<const DataTypeObjectToFetch &>(rhs);

    size_t size = elems.size();
    if (size != rhs_objecttofetch.elems.size())
        return false;

    for (size_t i = 0; i < size; ++i)
        if (!elems[i]->equals(*rhs_objecttofetch.elems[i]))
            return false;

    return true;
}


size_t DataTypeObjectToFetch::getPositionByName(const String & name) const
{
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i)
        if (names[i] == name)
            return i;
    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "ObjectToFetch doesn't have element with name '{}'", name);
}

String DataTypeObjectToFetch::getNameByPosition(size_t i) const
{
    if (i == 0 || i > names.size())
        throw Exception(ErrorCodes::ILLEGAL_INDEX, "Index of objecttofetchß element ({}) if out range ([1, {}])", i, names.size());

    return names[i - 1];
}


bool DataTypeObjectToFetch::textCanContainOnlyValidUTF8() const
{
    return std::all_of(elems.begin(), elems.end(), [](auto && elem) { return elem->textCanContainOnlyValidUTF8(); });
}

bool DataTypeObjectToFetch::haveMaximumSizeOfValue() const
{
    return std::all_of(elems.begin(), elems.end(), [](auto && elem) { return elem->haveMaximumSizeOfValue(); });
}

bool DataTypeObjectToFetch::isComparable() const
{
    return std::all_of(elems.begin(), elems.end(), [](auto && elem) { return elem->isComparable(); });
}

size_t DataTypeObjectToFetch::getMaximumSizeOfValueInMemory() const
{
    size_t res = 0;
    for (const auto & elem : elems)
        res += elem->getMaximumSizeOfValueInMemory();
    return res;
}

size_t DataTypeObjectToFetch::getSizeOfValueInMemory() const
{
    size_t res = 0;
    for (const auto & elem : elems)
        res += elem->getSizeOfValueInMemory();
    return res;
}

SerializationPtr DataTypeObjectToFetch::doGetDefaultSerialization() const
{
    SerializationObjectToFetch::ElementSerializations serializations(elems.size());
    for (size_t i = 0; i < elems.size(); ++i)
    {
        String elem_name = have_explicit_names ? names[i] : toString(i + 1);
        auto serialization = elems[i]->getDefaultSerialization();
        serializations[i] = std::make_shared<SerializationNamed>(serialization, elem_name);
    }

    return std::make_shared<SerializationObjectToFetch>(std::move(serializations), have_explicit_names);
}

SerializationPtr DataTypeObjectToFetch::getSerialization(const SerializationInfo & info) const
{
    SerializationObjectToFetch::ElementSerializations serializations(elems.size());
    const auto & info_tuple = assert_cast<const SerializationInfoObjectToFetch &>(info);

    for (size_t i = 0; i < elems.size(); ++i)
    {
        String elem_name = have_explicit_names ? names[i] : toString(i + 1);
        auto serialization = elems[i]->getSerialization(*info_tuple.getElementInfo(i));
        serializations[i] = std::make_shared<SerializationNamed>(serialization, elem_name);
    }

    return std::make_shared<SerializationObjectToFetch>(std::move(serializations), have_explicit_names);
}

MutableSerializationInfoPtr DataTypeObjectToFetch::createSerializationInfo(const SerializationInfo::Settings & settings) const
{
    MutableSerializationInfos infos;
    infos.reserve(elems.size());
    for (const auto & elem : elems)
        infos.push_back(elem->createSerializationInfo(settings));

    return std::make_shared<SerializationInfoObjectToFetch>(std::move(infos), settings);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "ObjectToFetch cannot be empty");

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    Strings names;
    names.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
    {
        if (const auto * name_and_type_pair = child->as<ASTNameTypePair>())
        {
            nested_types.emplace_back(DataTypeFactory::instance().get(name_and_type_pair->type));
            names.emplace_back(name_and_type_pair->name);
        }
        else
            nested_types.emplace_back(DataTypeFactory::instance().get(child));
    }

    if (names.empty())
        return std::make_shared<DataTypeObjectToFetch>(nested_types);
    else if (names.size() != nested_types.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Names are specified not for all elements of ObjectToFetch type");
    else
        return std::make_shared<DataTypeObjectToFetch>(nested_types, names);
}


void registerDataTypeObjectToFetch(DataTypeFactory & factory)
{
    factory.registerDataType("ObjectToFetch", create);
}

}
