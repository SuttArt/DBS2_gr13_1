// -------------------------------
// Group:
// Members:
// HU-Account names:
// -------------------------------
#include <memory>
#include <string>
#include <iostream>
#include <cstring>
#include <vector>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <experimental/filesystem>

#include "header/record.h"
#include "header/block.h"

Block::Block(std::string const& block_id) {
    // enforce length constraint
    if (block_id.size() != BLOCK_ID_SIZE) {
        throw std::invalid_argument("block_id must be exactly 5 bytes long.");
    }

    // try to read existing data
    data = load_data(block_id);
    dirty = false;

    // reserve data for new block
    if (!data) {
        // Allocate memory block
        char *buffer = new char[Block::BLOCK_SIZE];

        // add block id as first entry
        std::memcpy(buffer, block_id.c_str(), BLOCK_ID_SIZE);

        // set block dictionary to invalid offsets
        for (int i = 0; i < MAX_RECORDS; i++) {
            int val = -1;
            std::memcpy(buffer + BLOCK_ID_SIZE + i * sizeof(int), &val, sizeof(int));
        }

        data = std::shared_ptr<void>(buffer, [](void *ptr) { delete[] static_cast<char *>(ptr); });
        dirty = true;
    }
}

std::string Block::get_block_id() {
    char *ptr = static_cast<char *>(data.get());
    return std::string(ptr, ptr + BLOCK_ID_SIZE);
}

std::shared_ptr<Record> Block::add_record(std::vector<Record::Attribute> const &attributes) {
    // retrieve first invalid offset
    int offset_index = -1;

    for (int i = 0; i < MAX_RECORDS; i++) {
        int offset_val = *reinterpret_cast<int *>(static_cast<char *>(data.get()) + BLOCK_ID_SIZE + i * sizeof(int));

        if (offset_val == -1) {
            offset_index = i;
            break;
        }
    }

    // could not find empty space
    if (offset_index == -1)
        return nullptr;

    // find start position to write record
    int offset_val = BLOCK_ID_SIZE + MAX_RECORDS * sizeof(int);

    if (offset_index > 0) {
        // get position from last valid record
        int prev_offset_val = *reinterpret_cast<int *>(static_cast<char *>(data.get()) + BLOCK_ID_SIZE +
                                                (offset_index - 1) * sizeof(int));

        // read last record id
        int prev_record_offset_val = *reinterpret_cast<int *>(static_cast<char *>(data.get()) + prev_offset_val + sizeof(int));
        char *ptr = static_cast<char *>(data.get()) + prev_offset_val + prev_record_offset_val;
        std::string last_record_id = std::string(ptr, ptr + Record::RECORD_ID_SIZE);

        // set offset_val for new record
        std::shared_ptr<Record> last_record = get_record(last_record_id);
        int last_record_size = last_record->get_size();
        offset_val = prev_offset_val + last_record_size;
    }

    // create new record offset
    std::stringstream ss;
    ss << std::setw(5) << std::setfill('0') << offset_index;
    std::string record_offset = ss.str();

    // create new record
    std::string record_id = get_block_id() + record_offset;
    std::shared_ptr<Record> record = std::make_shared<Record>(Record(record_id, attributes));

    // empty space is not large enough
    if (offset_val + record->get_size() >= BLOCK_SIZE)
        return nullptr;

    // write record to block (and its offset to the directory)
    std::memcpy(static_cast<char *>(data.get()) + BLOCK_ID_SIZE + offset_index * sizeof(int), &offset_val, sizeof(int));
    std::memcpy(static_cast<char *>(data.get()) + offset_val, static_cast<char *>(record->get_data().get()), record->get_size());
    dirty = true;

    return record;
}

std::shared_ptr<Record> Block::get_record(std::string const& record_id)
{
    // enforce length constraint
    if (record_id.size() != Record::RECORD_ID_SIZE) {
        throw std::invalid_argument("record_id must be exactly 10 bytes long.");
    }

    // get directory offset
    std::string block_id = get_block_id(record_id);
    int offset_index = get_block_dictionary_offset(record_id);

    // enforce correct block id
    if (block_id != get_block_id()) {
        return nullptr;
    }

    // get record position in block
    int offset_val = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + BLOCK_ID_SIZE + offset_index * sizeof(int));

    // check for invalid or deleted records
    if (offset_val < 0)
        return nullptr;

    // extract record data
    int record_size = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + offset_val);

    char* buffer = new char[record_size];
    std::memcpy(buffer, static_cast<char *>(data.get()) + offset_val, record_size);

    // create record object
    std::shared_ptr<void> record_data = std::shared_ptr<void>(buffer, [](void* ptr) { delete[] static_cast<char*>(ptr); });
    return std::make_shared<Record>(Record(record_data));
}

bool Block::delete_record(std::string const& record_id)
{
    // enforce length constraint
    if (record_id.size() != Record::RECORD_ID_SIZE) {
        throw std::invalid_argument("record_id must be exactly 10 bytes long.");
    }

    // get directory offset
    std::string block_id = get_block_id();
    int offset_index = get_block_dictionary_offset(record_id);

    // enforce correct block id
    if (block_id != get_block_id()) {
        return false;
    }

    // get record position in block
    int offset_val = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + BLOCK_ID_SIZE + offset_index * sizeof(int));

    // check for invalid or deleted records
    if (offset_val < 0)
        return false;

    int record_size = *reinterpret_cast<int*>(static_cast<char*>(data.get()) + offset_val);

    // delete record dictionary entry and data
    int val = -2;
    std::memcpy(static_cast<char *>(data.get()) + BLOCK_ID_SIZE + offset_index * sizeof(int), &val, sizeof(int));

    std::stringstream ss;
    ss << std::setw(record_size) << std::setfill('0');
    std::memcpy(static_cast<char *>(data.get()) + offset_val, ss.str().c_str(), record_size);

    dirty = true;
    return true;
}

bool Block::is_dirty()
{
    return dirty;
}

bool Block::write_data()
{
    // Implement your solution here
    return false;
}

std::shared_ptr<void> Block::load_data(std::string const& block_id)
{
    // Create block directory (if needed)
    if (!std::experimental::filesystem::exists(BLOCK_DIR)) {
        if (!std::experimental::filesystem::create_directory(BLOCK_DIR)) {
            std::cerr << "Failed to create block directory." << std::endl;
        }
    }

    // open file handle
    std::ifstream file(BLOCK_DIR + block_id, std::ios::binary | std::ios::in);

    if (!file.is_open())
        return nullptr;

    // Allocate memory and read the file into it
    char* buffer = new char[BLOCK_SIZE];
    file.read(buffer, BLOCK_SIZE);

    // Handle partial read (error)
    if (!file) {
        delete[] buffer;
        file.close();
        return nullptr;
    }

    file.close();
    return std::shared_ptr<void>(buffer, [](void* ptr) { delete[] static_cast<char*>(ptr); });
}

std::string Block::create_block_id(int offset)
{
    // create new record
    std::stringstream ss;
    ss << std::setw(5) << std::setfill('0') << offset;
    std::string block_id = ss.str();

    if (block_id.size() > Block::BLOCK_ID_SIZE)
        throw std::runtime_error("Block id can at most have 5 bytes.");

    return block_id;
}

std::string Block::get_block_id(std::string const& record_id)
{
    if (record_id.size() != Record::RECORD_ID_SIZE) {
        throw std::invalid_argument("record_id must be exactly 10 bytes long.");
    }
    return record_id.substr(0, 5);
}

int Block::get_block_dictionary_offset(std::string const& record_id)
{
    std::string record_offset = record_id.substr(5, 5);

    std::stringstream ss;
    ss << record_offset;

    int dictionary_offset;
    ss >> dictionary_offset;

    return dictionary_offset;
}

