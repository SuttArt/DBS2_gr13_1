#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <memory>
#include <cassert>

#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"


static void test_record()
{
    std::cout << "[i] Testing record functionality." << std::endl;

    // create attributes
    std::string record_id = "0000000001";
    int a1 = 1;
    std::string a2 = "Test";
    bool a3 = true;

    // create records
    std::shared_ptr<Record> record = std::make_shared<Record>(Record(record_id, {a1, a2, a3}));

    // check that data is correctly contained
    assert(record->get_record_id() == record_id);
    assert(record->get_integer_attribute(1) == a1);
    assert(record->get_string_attribute(2) == a2);
    assert(record->get_boolean_attribute(3) == a3);
}

static void test_block()
{
    std::cout << "[i] Testing block functionality." << std::endl;

    // create block
    std::string block_id = "00000";
    std::shared_ptr<Block> block = std::make_shared<Block>(Block(block_id));
    std::vector<std::string> record_ids;

    // check that we can only insert MAX_RECORDS many records
    for (int i = 0; i < Block::MAX_RECORDS; i++)
    {
        record_ids.push_back(block->add_record({(int) i, (std::string) "Test", (bool) true})->get_record_id());
    }
    assert(block->is_dirty());
    assert(block->add_record({-1}) == nullptr);

    // check that we can correctly retrieve and delete all records
    for (int i = 0; i < Block::MAX_RECORDS; i++)
    {
        std::string record_id = record_ids.at(i);
        std::shared_ptr<Record> record = block->get_record(record_id);
        // check that we found the record
        assert(record != nullptr);

        // check that data is correctly contained
        assert(record->get_record_id() == record_id);
        assert(record->get_integer_attribute(1) == i);
        assert(record->get_string_attribute(2) == "Test");
        assert(record->get_boolean_attribute(3) == true);

        // check that the record can be deleted
        assert(block->delete_record(record_id));
        assert(block->get_record(record_id) == nullptr);
    }
}

static void test_block_read_write()
{
    std::cout << "[i] Testing block read/write functionality." << std::endl;

    // check that block does not exist yet
    std::string block_id = "00001";
    std::string block_path = "data/" + block_id;

    // delete block file if present
    std::ifstream file(block_path);
    if (file.good())
        assert(std::remove(block_path.c_str()) == 0);

    std::shared_ptr<Block> block = std::make_shared<Block>(Block(block_id));
    std::vector<std::string> record_ids;

    // add dummy records
    for (int i = 0; i < Block::MAX_RECORDS; i++)
        record_ids.push_back(block->add_record({(int) i, (std::string) "Test", (bool) true})->get_record_id());

    // write block
    assert(block->is_dirty());
    assert(block->write_data());
    assert(!block->is_dirty());

    // check that block can be read
    file = std::ifstream(block_path);
    assert(file.good());

    // reload block and check its contents
    block = std::make_shared<Block>(Block(block_id));

    for (int i = 0; i < Block::MAX_RECORDS; i++)
    {
        std::string record_id = record_ids.at(i);
        std::shared_ptr<Record> record = block->get_record(record_id);

        // check that data is correctly contained
        assert(record->get_record_id() == record_id);
        assert(record->get_integer_attribute(1) == i);
        assert(record->get_string_attribute(2) == "Test");
        assert(record->get_boolean_attribute(3) == true);
    }

    // delete the block
    assert(std::remove(block_path.c_str()) == 0);
}

static void test_buffer_manager()
{
    std::cout << "[i] Testing buffer manager functionality." << std::endl;

    // create 100 filled blocks
    int n_blocks = 100;
    std::vector<std::string> block_ids;

    for (int i = 0; i < n_blocks; i++)
    {
        std::string block_id = Block::create_block_id(i+2);
        std::string block_path = "data/" + block_id;

        // delete block file if present
        std::ifstream file(block_path);
        if (file.good())
            assert(std::remove(block_path.c_str()) == 0);

        // create block
        std::shared_ptr<Block> block = std::make_shared<Block>(Block(block_id));
        block_ids.push_back(block->get_block_id());

        // fill block with dummy data
        for (int k = 0; k < Block::MAX_RECORDS; k++)
            block->add_record({(int) k, (std::string) "Test", (bool) true});

        // write block
        block->write_data();
    }

    int n_cached_blocks = 10;
    std::shared_ptr<BufferManager> buffer = std::make_shared<BufferManager>(BufferManager(n_cached_blocks));

    // Test fixing blocks
    for (int i = 0; i < n_cached_blocks; i++)
    {
        std::string block_id = block_ids.at(i);
        std::shared_ptr<Block> block = buffer->fix_block(block_id);
        assert(block->get_block_id() == block_id);
    }

    // Test that we cannot fix any more blocks
    try {
        std::shared_ptr<Block> block = buffer->fix_block(block_ids.at(n_cached_blocks));
        assert(false);
    } catch (std::exception e) {
        assert(true);
    }

    // Test fixing blocks multiple times
    for (int i = 0; i < n_cached_blocks; i++)
    {
        std::string block_id = block_ids.at(i);
        std::shared_ptr<Block> block = buffer->fix_block(block_id);
        assert(block->get_block_id() == block_id);
    }

    // Test that we need two unfixes to remove a block
    for (int i = 0; i < n_cached_blocks; i++)
    {
        std::string block_id = block_ids.at(i);
        assert(buffer->unfix_block(block_id));

        // check that we cannot fix a new block yet
        try {
            std::shared_ptr<Block> block = buffer->fix_block(block_ids.at(n_cached_blocks + i));
            assert(false);
        } catch (std::exception const& e) {
            assert(true);
        }

        assert(buffer->unfix_block(block_id));

        // test that we cannot unfix a third time
        try {
            buffer->unfix_block(block_id);
            assert(false);
        } catch (std::exception const& e) {
            assert(true);
        }

        // fix a new block
        block_id = block_ids.at(n_cached_blocks + i);
        std::shared_ptr<Block> block = buffer->fix_block(block_id);
        assert(block->get_block_id() == block_id);
    }

    // delete blocks
    for (int i = 0; i < n_blocks; i++)
    {
        std::string block_id = Block::create_block_id(i+2);
        std::string block_path = "data/" + block_id;
        assert(std::remove(block_path.c_str()) == 0);
    }
}

int main() {
    test_record();
    test_block();
    test_block_read_write();
    test_buffer_manager();
    return 0;
}
