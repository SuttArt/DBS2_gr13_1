#include <memory>
#include <string>
#include <vector>
#include <variant>
#include <cassert>

#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"
#include "header/execution.h"
#include "header/bptree.h"

Table::Table(std::shared_ptr<BufferManager> const& buffer_manager, std::vector<std::string> const& block_ids)
    : buffer_manager(buffer_manager), block_ids(block_ids), current_block(0), current_record(0) {}

std::shared_ptr<Record> Table::next()
{
    // get first valid record
    while (current_block < block_ids.size())
    {
        std::string block_id = block_ids.at(current_block);
        std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

        if (block == nullptr)
            throw std::runtime_error("Cannot load table block: " + block_id);

        // get record from block
        std::shared_ptr<Record> record = block->get_record(Block::create_record_id(block_id, current_record));
        buffer_manager->unfix_block(block_id);

        // go to next record position
        if (current_record == Block::MAX_RECORDS - 1)
        {
            current_record = 0;
            ++current_block;
        } else
            ++current_record;

        // may be hole in the block, try next one
        if (record == nullptr)
            continue;

        return record;
    }

    // entire table read
    return nullptr;
}


Projection::Projection(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source,
                       std::vector<int> const& positions, std::vector<std::string> const& attribute_types)
    : buffer_manager(buffer_manager), source(source), positions(positions), attribute_types(attribute_types) {}

bool Projection::open()
{
    return source->open();
}

std::shared_ptr<Record> Projection::next()
{
    // get record from source
    std::shared_ptr<Record> record = source->next();

    // abort if no record available
    if (record == nullptr)
        return nullptr;

    // create projected record
    std::vector<Record::Attribute> values;

    for (int i = 0; i < positions.size(); i++)
    {
        int position = positions.at(i);
        std::string attribute_type = attribute_types.at(i);

        if (attribute_type == "int")
            values.push_back(record->get_integer_attribute(position));
        else if (attribute_type == "string")
            values.push_back(record->get_string_attribute(position));
        else if (attribute_type == "bool")
            values.push_back(record->get_boolean_attribute(position));
    }

    return std::make_shared<Record>(Record(record->get_record_id(), values));
}

bool Projection::close()
{
    return source->close();
}


Selection::Selection(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source,
int attribute_position, std::string const& attribute_type, Record::Attribute const& value, std::string const& comparator)
    : buffer_manager(buffer_manager), source(source), attribute_position(attribute_position),
    attribute_type(attribute_type), value(value), comparator(comparator)
{
    // check that attribute type and value type match
    if (attribute_type == "int")
        assert(std::holds_alternative<int>(value));
    else if (attribute_type == "string")
        assert(std::holds_alternative<std::string>(value));
    else if (attribute_type == "bool")
        assert(std::holds_alternative<bool>(value));
    else
        assert(false);

    // check that comparator is correctly defined (==, !=, <, <=, >, >=)
    if (comparator == "==" or comparator == "!=")
        assert(true); // valid for all data types
    else if (comparator == "<" or comparator == "<=" or comparator == ">" or comparator == ">=")
        assert(std::holds_alternative<int>(value)); // only works for numerical types
    else
        assert(false);
}

bool Selection::open()
{
    // Implement your solution here
    return false;
}

std::shared_ptr<Record> Selection::next()
{
    // Implement your solution here
    return nullptr;
}

bool Selection::close()
{
    // Implement your solution here
    return false;
}


Distinct::Distinct(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source)
    : buffer_manager(buffer_manager), source(source) {}

bool Distinct::open()
{
    // Implement your solution here
    return false;
}

std::shared_ptr<Record> Distinct::next()
{
    // Implement your solution here
    return nullptr;
}

bool Distinct::close()
{
    // Implement your solution here
    return false;
}