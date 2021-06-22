#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** ObjectToFetch user defined data type. Implemented as Tuple.
  * Used as result column when getObject() is called.
  *
  * Tuple elements can have names.
  * If an element is unnamed, it will have automatically assigned name like '1', '2', '3' corresponding to its position.
  * Manually assigned names must not begin with digit. Names must be unique.
  *
  * All tuples with same size and types of elements are equivalent for expressions, regardless to names of elements.
  */
class DataTypeObjectToFetch final : public IDataType
{
private:
    DataTypes elems;
    Strings names;
    bool have_explicit_names;
    bool serialize_names = true;
public:
    static constexpr bool is_parametric = true;

    DataTypeObjectToFetch(const DataTypes & elems);
    DataTypeObjectToFetch(const DataTypes & elems, const Strings & names, bool serialize_names_ = true);

    TypeIndex getTypeId() const override { return TypeIndex::ObjectToFetch; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "ObjectToFetch"; }

    bool canBeInsideNullable() const override { return false; }
    bool supportsSparseSerialization() const override { return true; }

    MutableColumnPtr createColumn() const override;
    MutableColumnPtr createColumn(const ISerialization & serialization) const override;

    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return !elems.empty(); }
    bool isComparable() const override;
    bool textCanContainOnlyValidUTF8() const override;
    bool haveMaximumSizeOfValue() const override;
    size_t getMaximumSizeOfValueInMemory() const override;
    size_t getSizeOfValueInMemory() const override;

    SerializationPtr doGetDefaultSerialization() const override;
    SerializationPtr getSerialization(const SerializationInfo & info) const override;
    MutableSerializationInfoPtr createSerializationInfo(const SerializationInfo::Settings & settings) const override;

    const DataTypePtr & getElement(size_t i) const { return elems[i]; }
    const DataTypes & getElements() const { return elems; }
    const Strings & getElementNames() const { return names; }

    size_t getPositionByName(const String & name) const;
    String getNameByPosition(size_t i) const;

    bool haveExplicitNames() const { return have_explicit_names; }
};

}

