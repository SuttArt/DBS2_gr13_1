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
#include "header/execution.h"


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

    // unfix blocks
    for (int i = 0; i < n_cached_blocks; i++)
    {
        std::string block_id = block_ids.at(n_cached_blocks + i);
        assert(buffer->unfix_block(block_id));
    }

    // delete blocks
    for (std::string const& block_id : block_ids)
        assert(buffer->erase_block(block_id));
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

static void test_bptree()
{
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
    int n_entries = 100;

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

    assert(bptree->erase());
}

static void test_query_execution()
{
    std::cout << "[i] Testing query execution functionality." << std::endl;

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    int n_cached_blocks = 10;
    std::shared_ptr<BufferManager> buffer = std::make_shared<BufferManager>(BufferManager(n_cached_blocks));

    // create 100 filled table blocks
    int n_blocks = 100;
    std::vector<std::string> block_ids;

    for (int i = 0; i < n_blocks; i++)
    {
        // create block
        std::shared_ptr<Block> block = buffer->fix_block(buffer->create_new_block());
        block_ids.push_back(block->get_block_id());

        // fill block with dummy data
        for (int k = 0; k < Block::MAX_RECORDS; k++)
            block->add_record({(int) k, (std::string) "Test", (bool) (k % 2 == 0)});

        buffer->unfix_block(block->get_block_id());
    }

    std::shared_ptr<Table> table = std::make_shared<Table>(Table(buffer, block_ids));

    // check that table can be correctly traversed
    assert(table->open());
    for (int i = 0; i < n_blocks; i++)
    {
        for (int k = 0; k < Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = table->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) == k);
            assert(record->get_string_attribute(2) == "Test");
            assert(record->get_boolean_attribute(3) == (k % 2 == 0));
        }
    }

    // check that table is fully read
    assert(table->next() == nullptr);
    assert(table->close());

    table = std::make_shared<Table>(Table(buffer, block_ids));
    std::shared_ptr<Projection> projection = std::make_shared<Projection>(Projection(buffer, table, {2,3}, {"string", "bool"}));

    // check that the projection is correct
    assert(projection->open());
    for (int i = 0; i < n_blocks; i++)
    {
        for (int k = 0; k < Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = projection->next();
            assert(record != nullptr);
            assert(record->get_string_attribute(1) == "Test");
            assert(record->get_boolean_attribute(2) == (k % 2 == 0));
        }
    }

    // check that projection is terminated
    assert(projection->next() == nullptr);
    assert(projection->close());

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {1}, {"int"}));
    std::shared_ptr<Selection> selection = std::make_shared<Selection>(Selection(buffer, projection, 1, "int", 5, std::string("==")));

    // check that the selection (== 5) is correct
    assert(selection->open());
    for (int i = 0; i < n_blocks; i++)
    {
        // check record values
        std::shared_ptr<Record> record = selection->next();
        assert(record != nullptr);
        assert(record->get_integer_attribute(1) == 5);
    }

    // check that selection is terminated
    assert(selection->next() == nullptr);
    assert(selection->close());

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {2,3}, {"string", "bool"}));
    selection = std::make_shared<Selection>(Selection(buffer, projection, 1, "string", (std::string) "Test", "!="));

    // check that selection is empty (!= Test)
    assert(selection->next() == nullptr);

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {1,2}, {"int", "string"}));
    selection = std::make_shared<Selection>(Selection(buffer, projection, 1, "int", 10, "<"));

    // check that the selection (< 10) is correct
    assert(selection->open());
    for (int i = 0; i < n_blocks; i++)
    {
        for (int k = 0; k < 10; k++)
        {
            // check record values
            std::shared_ptr<Record> record = selection->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) == k);
        }
    }

    // check that selection is terminated
    assert(selection->next() == nullptr);
    assert(selection->close());

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {1}, {"int"}));
    selection = std::make_shared<Selection>(Selection(buffer, projection, 1, "int", 15, "<="));

    // check that the selection (<= 15) is correct
    assert(selection->open());
    for (int i = 0; i < n_blocks; i++)
    {
        for (int k = 0; k <= 15; k++)
        {
            // check record values
            std::shared_ptr<Record> record = selection->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) == k);
        }
    }

    // check that selection is terminated
    assert(selection->next() == nullptr);
    assert(selection->close());

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {1, 1}, {"int", "int"}));
    selection = std::make_shared<Selection>(Selection(buffer, projection, 2, "int", 20, ">"));

    // check that the selection (> 20) is correct
    assert(selection->open());
    for (int i = 0; i < n_blocks; i++)
    {
        for (int k = 21; k < Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = selection->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) == k);
        }
    }

    // check that selection is terminated
    assert(selection->next() == nullptr);
    assert(selection->close());

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {1,3,2}, {"int", "bool", "string"}));
    selection = std::make_shared<Selection>(Selection(buffer, projection, 1, "int", 25, ">="));

    // check that the selection (>= 25) is correct
    for (int i = 0; i < n_blocks; i++)
    {
        for (int k = 25; k < Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = selection->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) == k);
        }
    }

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {1}, {"int"}));
    std::shared_ptr<Distinct> distinct = std::make_shared<Distinct>(Distinct(buffer, projection));

    // check that distinct is correct
    assert(distinct->open());
    for (int k = 0; k < Block::MAX_RECORDS; k++)
    {
        // check record values
        std::shared_ptr<Record> record = distinct->next();
        assert(record != nullptr);
        assert(record->get_integer_attribute(1) == k);
    }

    // check that distinct is terminated
    assert(distinct->next() == nullptr);
    assert(distinct->close());

    table = std::make_shared<Table>(Table(buffer, block_ids));
    projection = std::make_shared<Projection>(Projection(buffer, table, {2}, {"string"}));
    distinct = std::make_shared<Distinct>(Distinct(buffer, projection));

    // check that distinct is correct
    assert(distinct->open());

    // check record values
    std::shared_ptr<Record> record = distinct->next();
    assert(record != nullptr);
    assert(record->get_string_attribute(1) == "Test");

    // check that distinct is terminated
    assert(distinct->next() == nullptr);
    assert(distinct->close());
}

static void test_join() {
    std::cout << "[i] Testing join functionality." << std::endl;

    // delete existing block path if present
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    int n_cached_blocks = 10;
    std::shared_ptr<BufferManager> buffer = std::make_shared<BufferManager>(BufferManager(n_cached_blocks));

    // create 3 filled blocks for table 1
    int n_blocks = 3;
    int pk1 = 0;
    std::vector<std::string> block_ids1;

    for (int i = 0; i < n_blocks; i++) {
        // create block
        std::shared_ptr<Block> block = buffer->fix_block(buffer->create_new_block());
        block_ids1.push_back(block->get_block_id());

        // fill block with dummy data
        for (int k = 0; k < Block::MAX_RECORDS; k++) {
            block->add_record({(int) pk1, (std::string) "Test", (bool) (k % 2 == 0)});
            pk1++;
        }

        buffer->unfix_block(block->get_block_id());
    }

    // create 3 filled blocks for table 2
    std::vector<std::string> block_ids2;
    int pk2 = 0;

    for (int i = 0; i < n_blocks; i++) {
        // create block
        std::shared_ptr<Block> block = buffer->fix_block(buffer->create_new_block());
        block_ids2.push_back(block->get_block_id());

        // fill block with dummy data
        for (int k = 0; k < Block::MAX_RECORDS; k++) {
            block->add_record({(int) pk2, (std::string) "Test", (bool) (k % 2 == 1)});
            pk2++;
        }

        buffer->unfix_block(block->get_block_id());
    }

    std::shared_ptr<Table> table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    std::shared_ptr<Table> table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    std::shared_ptr<Join> join = std::make_shared<Join>(Join(buffer,
        table1, table2, 1, 1,
        {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, "==") // "" for record id
    );

    // check that equi-join with integers is correct
    assert(join->open());
    for (int i = 0; i < n_blocks; i++)
    {
        for (int k = 0; k < Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);

            assert(record->get_integer_attribute(1) == record->get_integer_attribute(4));
            assert(record->get_string_attribute(2) == "Test");
            assert(record->get_string_attribute(5) == "Test");
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());

    table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    join = std::make_shared<Join>(Join(buffer,
       table1, table2,2, 2,
       {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, "==") // "" for record id
    );

    // check that equi-join with strings is correct
    assert(join->open());
    for (int i = 0; i < n_blocks*n_blocks; i++)
    {
        for (int k = 0; k < Block::MAX_RECORDS*Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);

            assert(record->get_string_attribute(2) == "Test");
            assert(record->get_string_attribute(5) == "Test");
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());

    table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    join = std::make_shared<Join>(Join(buffer,
       table1, table2,3, 3,
       {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, "==") // "" for record id
    );

    // check that equi-join with booleans is correct
    assert(join->open());
    for (int i = 0; i < n_blocks*n_blocks; i++)
    {
        for (int k = 0; k < Block::MAX_RECORDS*Block::MAX_RECORDS/2; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);

            assert(record->get_string_attribute(3) == record->get_string_attribute(6));
            assert(record->get_string_attribute(2) == "Test");
            assert(record->get_string_attribute(5) == "Test");
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());

    table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    join = std::make_shared<Join>(Join(buffer,
       table1, table2,1, 1,
       {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, "!=") // "" for record id
    );

    // check that not equi-join with integers is correct
    assert(join->open());
    for (int i = 0; i < n_blocks*Block::MAX_RECORDS; i++)
    {
        for (int k = 0; k < n_blocks*Block::MAX_RECORDS-1; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) != record->get_integer_attribute(4));
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());

    table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    join = std::make_shared<Join>(Join(buffer,
       table1, table2,1, 1,
       {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, "<") // "" for record id
    );

    // check that lesser-join with integers is correct
    assert(join->open());
    for (int i = 0; i < n_blocks*Block::MAX_RECORDS; i++)
    {
        for (int k = i+1; k < n_blocks*Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) < record->get_integer_attribute(4));
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());

    table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    join = std::make_shared<Join>(Join(buffer,
        table1, table2,1, 1,
        {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, ">") // "" for record id
    );

    // check that greater-join with integers is correct
    assert(join->open());
    for (int i = 0; i < n_blocks*Block::MAX_RECORDS; i++)
    {
        for (int k = 0; k < i; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) > record->get_integer_attribute(4));
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());

    table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    join = std::make_shared<Join>(Join(buffer,
       table1, table2,1, 1,
       {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, "<=") // "" for record id
    );

    // check that lesser-equal-join with integers is correct
    assert(join->open());
    for (int i = 0; i < n_blocks*Block::MAX_RECORDS; i++)
    {
        for (int k = i; k < n_blocks*Block::MAX_RECORDS; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) <= record->get_integer_attribute(4));
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());

    table1 = std::make_shared<Table>(Table(buffer, block_ids1));
    table2 = std::make_shared<Table>(Table(buffer, block_ids2));

    join = std::make_shared<Join>(Join(buffer,
        table1, table2,1, 1,
        {"", "int", "string", "bool"}, {"", "int", "string", "bool"}, ">=") // "" for record id
    );

    // check that greater-equal-join with integers is correct
    assert(join->open());
    for (int i = 0; i < n_blocks*Block::MAX_RECORDS; i++)
    {
        for (int k = 0; k < i+1; k++)
        {
            // check record values
            std::shared_ptr<Record> record = join->next();
            assert(record != nullptr);
            assert(record->get_integer_attribute(1) >= record->get_integer_attribute(4));
        }
    }

    // check that join is terminated
    assert(join->next() == nullptr);
    assert(join->close());
}


int main() {
    test_record();
    test_block();
    test_block_read_write();
    test_buffer_manager();
    test_bptree_node();
    test_bptree();
    test_query_execution();
    test_join();

    // delete existing block path
    if (std::filesystem::exists(Block::BLOCK_DIR) && std::filesystem::is_directory(Block::BLOCK_DIR))
        std::filesystem::remove_all(Block::BLOCK_DIR);

    return 0;
}
