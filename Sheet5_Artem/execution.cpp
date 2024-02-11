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

bool Table::open()
{
    current_block = 0;
    current_record = 0;
    return true;
}

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

bool Table::close()
{
    current_block = 0;
    current_record = 0;
    return true;
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
    return source->open();
}

std::shared_ptr<Record> Selection::next()
{
    // get record from source
    std::shared_ptr<Record> record = source->next();

    while (record)
    {
        // get correct attribute
        Record::Attribute attribute;

        if (attribute_type == "int")
            attribute = record->get_integer_attribute(attribute_position);
        else if (attribute_type == "string")
            attribute = record->get_string_attribute(attribute_position);
        else if (attribute_type == "bool")
            attribute = record->get_boolean_attribute(attribute_position);

        // compare attribute with value
        bool comparison_result = false;

        if (comparator == "==")
            comparison_result = attribute == value;
        else if (comparator == "!=")
            comparison_result = attribute != value;
        else if (comparator == "<")
            comparison_result = attribute < value;
        else if (comparator == "<=")
            comparison_result = attribute <= value;
        else if (comparator == ">")
            comparison_result = attribute > value;
        else if (comparator == ">=")
            comparison_result = attribute >= value;

        if (comparison_result)
            return record;

        record = source->next();
    }

    return nullptr;
}

bool Selection::close()
{
    return source->close();
}


Distinct::Distinct(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source)
    : buffer_manager(buffer_manager), source(source) {}

bool Distinct::open()
{
    bptree = std::make_shared<BPTree>(BPTree(buffer_manager, buffer_manager->create_new_block()));
    return source->open();
}

std::shared_ptr<Record> Distinct::next()
{
    // get record from source
    std::shared_ptr<Record> record = source->next();

    while (record)
    {
        int record_hash = record->get_hash();

        // check if record is known
        if (!bptree->search_record(record_hash).has_value())
        {
            bptree->insert_record(record_hash, "----------");
            return record;
        }

        record = source->next();
    }

    return nullptr;
}

bool Distinct::close()
{
    return bptree->erase() && source->close();
}

Join::Join(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source1,
           std::shared_ptr<QueryOperator> const& source2, int attribute_position1, int attribute_position2,
           std::vector<std::string> const& attribute_types1, std::vector<std::string> const& attribute_types2,
           std::string const& comparator) : buffer_manager(buffer_manager), source1(source1), source2(source2),
           attribute_position1(attribute_position1), attribute_position2(attribute_position2),
           attribute_types1(attribute_types1), attribute_types2(attribute_types2), comparator(comparator)
{
    // check that join attribute types are the same
    assert(attribute_types1[attribute_position1] == attribute_types2[attribute_position2]);

    // check that comparator is correctly defined (==, !=, <, <=, >, >=)
    if (comparator == "==" or comparator == "!=")
        assert(true); // valid for all data types
    else if (comparator == "<" or comparator == "<=" or comparator == ">" or comparator == ">=")
        assert(attribute_types1[attribute_position1] == "int"); // only works for numerical types
    else
        assert(false);
}

bool Join::open()
{

    Record::Attribute attribute_to_compare_source1;
    Record::Attribute attribute_to_compare_source2;
    bool comparison_result = false;

    // create new block
    tmp_block_id = buffer_manager->create_new_block();
    // save block id
    tmp_block_ids.push_back(tmp_block_id);

    source1->open();
    // get record from source1
    std::shared_ptr<Record> record_source1 = source1->next();
    while (record_source1)
    {
        std::string attribute_type1 = attribute_types1.at(attribute_position1);
        std::string attribute_type2 = attribute_types2.at(attribute_position2);

        // define attribute from source 1
        if (attribute_type1 == "int")
            attribute_to_compare_source1 = record_source1->get_integer_attribute(attribute_position1);
        else if (attribute_type1 == "string")
            attribute_to_compare_source1 = record_source1->get_string_attribute(attribute_position1);
        else if (attribute_type1 == "bool")
            attribute_to_compare_source1 = record_source1->get_boolean_attribute(attribute_position1);

        source2->open();
        // get record from source2
        std::shared_ptr<Record> record_source2 = source2->next();
        while (record_source2)
        {
            // define attribute from source 1
            if (attribute_type2 == "int")
                attribute_to_compare_source2 = record_source2->get_integer_attribute(attribute_position2);
            else if (attribute_type2 == "string")
                attribute_to_compare_source2 = record_source2->get_string_attribute(attribute_position2);
            else if (attribute_type2 == "bool")
                attribute_to_compare_source2 = record_source2->get_boolean_attribute(attribute_position2);

            if (comparator == "==")
                comparison_result = attribute_to_compare_source1 == attribute_to_compare_source2;
            else if (comparator == "!=")
                comparison_result = attribute_to_compare_source1 != attribute_to_compare_source2;
            else if (comparator == "<")
                comparison_result = attribute_to_compare_source1 < attribute_to_compare_source2;
            else if (comparator == "<=")
                comparison_result = attribute_to_compare_source1 <= attribute_to_compare_source2;
            else if (comparator == ">")
                comparison_result = attribute_to_compare_source1 > attribute_to_compare_source2;
            else if (comparator == ">=")
                comparison_result = attribute_to_compare_source1 >= attribute_to_compare_source2;

            //if true -> write to block
            if (comparison_result)
            {
                // get attributes from source tuples
                std::vector<std::variant<int, std::string, bool>> attributes_to_add;

                for (size_t i = 0; i < attribute_types1.size(); i++) {
                    if (attribute_types1[i] == "int")
                        attributes_to_add.push_back(record_source1->get_integer_attribute(i));
                    else if (attribute_types1[i] == "string")
                        attributes_to_add.push_back(record_source1->get_string_attribute(i));
                    else if (attribute_types1[i] == "bool")
                        attributes_to_add.push_back(record_source1->get_boolean_attribute(i));
                }

                for (size_t i = 0; i < attribute_types2.size(); i++) {
                    if (attribute_types2[i] == "int")
                        attributes_to_add.push_back(record_source2->get_integer_attribute(i));
                    else if (attribute_types2[i] == "string")
                        attributes_to_add.push_back(record_source2->get_string_attribute(i));
                    else if (attribute_types2[i] == "bool")
                        attributes_to_add.push_back(record_source2->get_boolean_attribute(i));
                }

                // fix block
                std::shared_ptr<Block> tmp_block = buffer_manager->fix_block(tmp_block_id);
                // write data to block
                std::shared_ptr<Record> tmp_record = tmp_block->add_record(attributes_to_add);

                // block is full, new block required
                if (tmp_record == nullptr)
                {
                    // unfix old block
                    buffer_manager->unfix_block(tmp_block->get_block_id());
                    // create new block
                    tmp_block_id = buffer_manager->create_new_block();
                    // save block id
                    tmp_block_ids.push_back(tmp_block_id);
                    // fix new block
                    std::shared_ptr<Block> tmp_block = buffer_manager->fix_block(tmp_block_id);
                    // write data to new block
                    tmp_block->add_record(attributes_to_add);
                }

                //unfix block
                buffer_manager->unfix_block(tmp_block_id);
            }

            record_source2 = source2->next();
        }
        source2->close();
        record_source1 = source1->next();
    }
    source1->close();

    tmp_table = std::make_shared<Table>(Table(buffer_manager, tmp_block_ids));

    return tmp_table->open();
}

std::shared_ptr<Record> Join::next()
{
    return tmp_table->next();;
}

bool Join::close()
{
    for (std::string block_id : tmp_block_ids) {
        buffer_manager->erase_block(block_id);
    }

    return tmp_table->close();
}