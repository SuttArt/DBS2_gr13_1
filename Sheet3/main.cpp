#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <random>
#include <memory>
#include <cassert>
#include <algorithm>

#include "header/filesystem.h"
#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"
#include "header/bptree.h"


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

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

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

    // check that we can correctly retrieve, update and delete all records
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

        // check that the data can be changed
        std::shared_ptr<Record> updated_record = std::make_shared<Record>(Record(record_id, {(int) i, (std::string) "test", (bool) false}));
        block->update_record(updated_record);
        record = block->get_record(record_id);

        // check that data is correctly contained
        assert(record->get_record_id() == record_id);
        assert(record->get_integer_attribute(1) == i);
        assert(record->get_string_attribute(2) == "test");
        assert(record->get_boolean_attribute(3) == false);

        // check that the record can be deleted
        assert(block->delete_record(record_id));
        assert(block->get_record(record_id) == nullptr);
    }
}

static void test_block_read_write()
{
    std::cout << "[i] Testing block read/write functionality." << std::endl;

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    // check that block does not exist yet
    std::string block_id = "00001";

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    std::shared_ptr<Block> block = std::make_shared<Block>(Block(block_id));
    std::vector<std::string> record_ids;

    // add dummy records
    for (int i = 0; i < Block::MAX_RECORDS; i++)
        record_ids.push_back(block->add_record({(int) i, (std::string) "Test", (bool) true})->get_record_id());

    // write block
    assert(block->is_dirty());
    block->write_data();
    assert(!block->is_dirty());

    // check that block can be read
    std::ifstream file(Block::BLOCK_DIR);
    file = std::ifstream(Block::BLOCK_DIR);
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
    assert(std::remove((Block::BLOCK_DIR + block_id).c_str()) == 0);
}

static void test_buffer_manager()
{
    std::cout << "[i] Testing buffer manager functionality." << std::endl;

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    // create 100 filled blocks
    int n_blocks = 100;
    std::vector<std::string> block_ids;

    for (int i = 0; i < n_blocks; i++)
    {
        std::string block_id = Block::create_block_id(i+2);
        std::string block_path = Block::BLOCK_DIR + block_id;

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

    // test fixing blocks
    for (int i = 0; i < n_cached_blocks; i++)
    {
        std::string block_id = block_ids.at(i);
        std::shared_ptr<Block> block = buffer->fix_block(block_id);
        assert(block->get_block_id() == block_id);
    }

    // test that we cannot fix any more blocks
    try
    {
        std::shared_ptr<Block> block = buffer->fix_block(block_ids.at(n_cached_blocks));
        assert(false);
    } catch (std::exception const& e)
    {
        assert(true);
    }

    // test fixing blocks multiple times
    for (int i = 0; i < n_cached_blocks; i++)
    {
        std::string block_id = block_ids.at(i);
        std::shared_ptr<Block> block = buffer->fix_block(block_id);
        assert(block->get_block_id() == block_id);
    }

    // test that we need two unfixes to remove a block
    for (int i = 0; i < n_cached_blocks; i++)
    {
        std::string block_id = block_ids.at(i);
        assert(buffer->unfix_block(block_id));

        // check that we cannot fix a new block yet
        try
        {
            std::shared_ptr<Block> block = buffer->fix_block(block_ids.at(n_cached_blocks + i));
            assert(false);
        } catch (std::exception const& e)
        {
            assert(true);
        }

        assert(buffer->unfix_block(block_id));

        // test that we cannot unfix a third time
        try {
            buffer->unfix_block(block_id);
            assert(false);
        } catch (std::exception const& e)
        {
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
        std::string block_path = Block::BLOCK_DIR + block_id;
        assert(std::remove(block_path.c_str()) == 0);
    }
}

static void test_bptree_node()
{
    std::cout << "[i] Testing BP tree node functionality." << std::endl;

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    int n_cached_blocks = 10;
    std::shared_ptr<BufferManager> buffer = std::make_shared<BufferManager>(BufferManager(n_cached_blocks));

    // create node information
    std::string node_id = buffer->create_new_block();
    std::string parent_id = buffer->create_new_block();
    bool leaf = false;

    std::string block_path = Block::BLOCK_DIR + node_id;
    std::shared_ptr<BPTreeNode> node = BPTreeNode::create_node(buffer, node_id, parent_id, leaf);

    // test new attributes
    assert(node->get_parent_id() == parent_id);
    assert(node->is_leaf() == leaf);
    assert(node->get_values().size() == 0);
    assert(node->get_children_ids().size() == 0);

    // change values
    std::vector<int> values = node->get_values();

    for (int i = 0; i < BPTreeNode::MAX_VALUES; i++)
        values.push_back(i);

    node->change_values(values);
    values = node->get_values();

    assert(values.size() == BPTreeNode::MAX_VALUES);

    for (int i = 0; i < BPTreeNode::MAX_VALUES; i++)
        assert(values.at(i) == i);

    // change children ids
    std::vector<std::string> children_ids = node->get_children_ids();

    for (int i = 0; i < BPTreeNode::MAX_CHILDREN; i++)
        children_ids.push_back(Block::create_record_id(node_id, i));

    node->change_children_ids(children_ids);
    children_ids = node->get_children_ids();

    assert(children_ids.size() == BPTreeNode::MAX_CHILDREN);

    for (int i = 0; i < BPTreeNode::MAX_CHILDREN; i++)
        assert(children_ids.at(i) == Block::create_record_id(node_id, i));

    // change parent_id
    parent_id = buffer->create_new_block();
    assert(node->change_parent_id(parent_id));
    assert(node->get_parent_id() == parent_id);
}

static void test_bptree() {
    std::cout << "[i] Testing BP tree functionality." << std::endl;

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    int n_cached_blocks = 10;
    std::shared_ptr<BufferManager> buffer = std::make_shared<BufferManager>(BufferManager(n_cached_blocks));

    // create node information
    std::string root_node_id = buffer->create_new_block();

    // test BP tree insert
    std::shared_ptr<BPTree> bptree = std::make_shared<BPTree>(buffer, root_node_id);
    int n_entries = 10000;

    // Initialize Mersenne Twister pseudo-random number generator with a seed
    std::mt19937 gen(1379);

    // generate random integers
    std::vector<int> numbers;

    for (int i = 0; i < n_entries; i++)
        numbers.push_back(i);

    // Shuffle the vector using the generator
    std::shuffle(numbers.begin(), numbers.end(), gen);

    for (int i : numbers)
    {
        std::string record_id = Block::create_record_id("-----", i);
        assert(bptree->insert_record(i, record_id));
    }

    // check serialization by reloading tree
    root_node_id = bptree->get_root_node_id();
    bptree = std::make_shared<BPTree>(buffer, root_node_id);

    // test BP tree search
    for (int i : numbers)
        assert(bptree->search_record(i) == Block::create_record_id("-----", i));

    // check that duplicates are not allowed
    try
    {
        std::string record_id = Block::create_record_id("-----", 0);
        bptree->insert_record(0, record_id);
        assert(false);
    } catch (std::exception const& e)
    {
        assert(true);
    }

}

int main() {
    test_record();
    test_block();
    test_block_read_write();
    test_buffer_manager();
    test_bptree_node();
    test_bptree();

    // delete existing block path
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    return 0;
}
