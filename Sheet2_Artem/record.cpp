#include <memory>
#include <string>
#include <iostream>
#include <cstring>
#include <vector>

#include "header/record.h"

Record::Record(std::shared_ptr<void> const& data) : data(data) {}

Record::Record(std::string const& record_id, std::vector<Attribute> const& attributes)
{
    // Check record_id shape
    if (record_id.size() != Record::RECORD_ID_SIZE) {
        throw std::invalid_argument("record_id must be exactly 10 bytes long.");
    }

    // Calculate total size needed
    // = record size field size + dictionary size + record_id + attribute sizes
    int dictionary_size = (attributes.size() + 2) * sizeof(int);

    int size = sizeof(int) + dictionary_size;
    size += Record::RECORD_ID_SIZE;

    for (const auto& attr : attributes) {
        if (std::holds_alternative<int>(attr)) {
            size += sizeof(int);
        } else if (std::holds_alternative<std::string>(attr)) {
            size += std::get<std::string>(attr).size();
        } else if (std::holds_alternative<bool>(attr)) {
            size += sizeof(bool);
        }
    }

    // Allocate memory block
    char* buffer = new char[size];

    // Offset for copying data
    int dictionary_offset = sizeof(int);
    int offset = sizeof(int) + dictionary_size;

    // Copy size of record
    std::memcpy(buffer, &size, sizeof(int));

    // Copy record ID as first attribute
    std::memcpy(buffer + dictionary_offset, &offset, sizeof(int));
    std::memcpy(buffer + offset, record_id.c_str(), Record::RECORD_ID_SIZE);

    dictionary_offset += sizeof(int);
    offset += Record::RECORD_ID_SIZE;

    // Copy attributes
    for (const auto& attr : attributes) {
        std::memcpy(buffer + dictionary_offset, &offset, sizeof(int));
        dictionary_offset += sizeof(int);

        if (std::holds_alternative<int>(attr)) {
            int val = std::get<int>(attr);
            std::memcpy(buffer + offset, &val, sizeof(int));
            offset += sizeof(int);
        } else if (std::holds_alternative<std::string>(attr)) {
            const std::string& val = std::get<std::string>(attr);
            std::memcpy(buffer + offset, val.c_str(), val.size());
            offset += val.size();
        } else if (std::holds_alternative<bool>(attr)) {
            bool val = std::get<bool>(attr);
            std::memcpy(buffer + offset, &val, sizeof(bool));
            offset += sizeof(bool);
        }
    }

    // Store end offset in dictionary
    std::memcpy(buffer + dictionary_offset, &offset, sizeof(int));

    // Create shared_ptr
    data = std::shared_ptr<void>(buffer, [](void* ptr) { delete[] static_cast<char*>(ptr); });
}

std::string Record::get_record_id()
{
    return get_string_attribute(0);
}

std::string Record::get_string_attribute(int attribute_index)
{
    if (data)
    {
        // get attribute start position
        int position = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + sizeof(int) + attribute_index * sizeof(int));

        // get (next) attribute start position
        int next_position = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + sizeof(int) + (attribute_index+1) * sizeof(int));

        // retrieve the data
        char *ptr = static_cast<char *>(data.get()) + position;
        return std::string(ptr, ptr + next_position - position);
    }
    return "";
}

int Record::get_integer_attribute(int attribute_index)
{
    if (data)
    {
        // get attribute start position
        int position = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + sizeof(int) + attribute_index * sizeof(int));
        // retrieve the data
        return *reinterpret_cast<int*>(static_cast<char*>(data.get()) + position);
    }
    return 0;
}

bool Record::get_boolean_attribute(int attribute_index)
{
    if (data)
    {
        // get attribute start position
        int position = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + sizeof(int) + attribute_index * sizeof(int));
        // retrieve the data
        return *reinterpret_cast<bool*>(static_cast<char*>(data.get()) + position);;
    }
    return false;
}

std::shared_ptr<void> Record::get_data()
{
    return data;
}

int Record::get_size()
{
    return *reinterpret_cast<int*>(static_cast<char*>(data.get()));
}
