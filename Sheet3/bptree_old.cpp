// -------------------------------

// -------------------------------
#include <memory>
#include <string>
#include <cassert>
#include <optional>
#include <vector>

#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"
#include "header/bptree.h"

// got the following error: std has no member 'invalid_argument'
#include <stdexcept>
#include <algorithm>
#include <iostream>


BPTreeNode::BPTreeNode(std::shared_ptr<BufferManager> const& buffer_manager, std::string const& node_id)
: buffer_manager(buffer_manager), block_id(node_id) {}

std::string BPTreeNode::get_node_id()
{
    return block_id;
}

std::string BPTreeNode::get_parent_id()
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + block_id);

    // get parent id
    std::string parent_id = block->get_record(Block::create_record_id(block_id, 0))->get_string_attribute(1);

    buffer_manager->unfix_block(block_id);
    return parent_id;
}

bool BPTreeNode::is_leaf()
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + block_id);

    // get leaf information
    bool leaf = block->get_record(Block::create_record_id(block_id, 1))->get_boolean_attribute(1);

    buffer_manager->unfix_block(block_id);
    return leaf;
}

std::vector<int> BPTreeNode::get_values()
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + block_id);

    // get number of values
    int n_values = block->get_record(Block::create_record_id(block_id, 2))->get_integer_attribute(1);
    std::vector<int> values;

    // get values
    for (int i = 0; i < n_values; i++)
        values.push_back(block->get_record(Block::create_record_id(block_id, 3 + i))->get_integer_attribute(1));

    buffer_manager->unfix_block(block_id);
    return values;
}

std::vector<std::string> BPTreeNode::get_children_ids()
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + block_id);

    // get number of children
    int n_children = block->get_record(Block::create_record_id(block_id, 32))->get_integer_attribute(1);
    std::vector<std::string> children;

    // get children
    for (int i = 0; i < n_children; i++)
        children.push_back(block->get_record(Block::create_record_id(block_id, 33 + i))->get_string_attribute(1));

    buffer_manager->unfix_block(block_id);
    return children;
}

bool BPTreeNode::change_parent_id(std::string parent_id)
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + block_id);

    // change n_values
    std::shared_ptr<Record> record = std::make_shared<Record>(Record(Block::create_record_id(block_id, 0), {(std::string) parent_id}));

    if (!block->update_record(record))
        return false;

    buffer_manager->unfix_block(block_id);
    return true;
}

bool BPTreeNode::change_values(std::vector<int> const& values)
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + block_id);

    if (values.size() > BPTreeNode::MAX_VALUES)
        throw std::invalid_argument("Cannot have more index block values than " + std::to_string(BPTreeNode::MAX_VALUES));

    // enforce order
    for (int i = 1; i < values.size(); i++)
    {
        if (values.at(i) < values.at(i-1))
            throw std::invalid_argument("Cannot have unsorted values in index block: " + block_id);
    }

    // change number of values
    std::shared_ptr<Record> record = std::make_shared<Record>(
            Record(Block::create_record_id(block_id, 2), {(int) values.size()}));

    if (!block->update_record(record))
        return false;

    // change values
    for (int i = 0; i < values.size(); i++)
    {
        std::string record_id = Block::create_record_id(block_id, 3 + i);
        std::shared_ptr<Record> record = std::make_shared<Record>(Record(record_id, {(int) values.at(i)}));

        if (!block->update_record(record))
            return false;
    }

    buffer_manager->unfix_block(block_id);
    return true;
}

bool BPTreeNode::change_children_ids(std::vector<std::string> const& children_ids)
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(block_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + block_id);

    if (children_ids.size() > BPTreeNode::MAX_CHILDREN)
        throw std::invalid_argument("Cannot have more index block children than " + std::to_string(BPTreeNode::MAX_CHILDREN));

    // change number of children
    std::shared_ptr<Record> record = std::make_shared<Record>(
            Record(Block::create_record_id(block_id, 32), {(int) children_ids.size()}));

    if (!block->update_record(record))
        return false;

    // change children
    for (int i = 0; i < children_ids.size(); i++)
    {
        std::string record_id = Block::create_record_id(block_id, 33 + i);
        std::shared_ptr<Record> record = std::make_shared<Record>(Record(record_id, {(std::string) children_ids.at(i)}));

        if (!block->update_record(record))
            return false;
    }

    buffer_manager->unfix_block(block_id);
    return true;
}

std::shared_ptr<BPTreeNode> BPTreeNode::create_node(std::shared_ptr<BufferManager> const& buffer_manager, std::string const& node_id, std::string parent_id, bool leaf)
{
    std::shared_ptr<Block> block = buffer_manager->fix_block(node_id);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load index block: " + node_id);

    if (!block->is_dirty())
        throw std::invalid_argument("Index block already exists: " + node_id);

    int n_records = Block::MAX_RECORDS;

    // add parent id
    std::shared_ptr<Record> r = block->add_record({(std::string) parent_id});
    n_records -= 1;

    if (r == nullptr)
        throw std::invalid_argument("Cannot add parent id in " + node_id);

    // add leaf flag
    r = block->add_record({(bool) leaf});
    n_records -= 1;

    if (r == nullptr)
        throw std::invalid_argument("Cannot add leaf flag in " + node_id);

    // add amount of valid values
    r = block->add_record({(int) 0});
    n_records -= 1;

    if (r == nullptr)
        throw std::invalid_argument("Cannot add number of values in " + node_id);

    // add dummy values
    for (int i = 0; i < (n_records - 1)/2 - 1; i++)
    {
        r = block->add_record({(int) -1});

        if (r == nullptr)
            throw std::invalid_argument("Cannot add dummy values in " + node_id);
    }

    n_records -= (n_records - 1)/2 - 1;

    // add amount of valid pointers
    r = block->add_record({(int) 0});
    n_records -= 1;

    if (r == nullptr)
        throw std::invalid_argument("Cannot add number of pointers in " + node_id);

    // add dummy pointers
    for (int i = 0; i < n_records; i++)
    {
        // create dummy pointer
        r = block->add_record({ std::string(Record::RECORD_ID_SIZE, '0') });

        if (r == nullptr)
            throw std::invalid_argument("Cannot add dummy pointers in " + node_id);
    }

    buffer_manager->unfix_block(block->get_block_id());
    return std::make_shared<BPTreeNode>(buffer_manager, node_id);
}

std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> BPTreeNode::insert_record(int attribute, std::string const& record_id)
{
    //get present values and record ids
    std::vector<int> values = get_values();
    std::vector<std::string> record_ids = get_children_ids();

    std::cout << record_ids.size() << " " << values.size() << "\n";

    //proof if record is already present (duplicates)
    if(std::find(values.begin(), values.end(), attribute) != values.end()){
        throw std::invalid_argument("Duplicate value not allowed");
    }
    std::string next_leaf_ptr;
    bool leaf_ptr = false;

    for (const auto& item : record_ids) {
    std::cout << item << " ";
    }
    std::cout << std::endl;

    // Check if the record_ids vector is not empty
    if (!record_ids.empty()) {
        // Get the last element
        next_leaf_ptr = record_ids.back();
        std::cout << "back id: " << next_leaf_ptr << "\n";

        // Check if the last value is really a leaf pointer and not a record ID
        if (next_leaf_ptr.find('-') == std::string::npos) {
            // Remove the pointer as it is a leaf pointer
            record_ids.pop_back();
            leaf_ptr = true;
        }
    }


    // Find the correct position to insert
    auto it = std::lower_bound(values.begin(), values.end(), attribute);
    int index = it - values.begin();

    std::cout << "attr: " << attribute << "\n";
    std::cout << "index: " << index << "\n";
    if(index-1 > 0){
    std::cout << "previous: " << values[index-1] << ", next: " << values[index] << "\n";
    }
    // Insert the attribute and record ID at the found position
    //if (index < values.size()){
        values.insert(it, attribute);
        record_ids.insert(record_ids.begin() + index, record_id);
    /*}else{
        values.push_back(attribute);
        if(record_ids.size() > 1){
            record_ids.insert(record_ids.end(), record_id);
        }else{std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> BPTreeNode::insert_record(int attribute, std::string const& record_id)
{
    //get present values and record ids
    std::vector<int> values = get_values();
    std::vector<std::string> record_ids = get_children_ids();

    std::cout << record_ids.size() << " " << values.size() << "\n";

    //proof if record is already present (duplicates)
    if(std::find(values.begin(), values.end(), attribute) != values.end()){
        throw std::invalid_argument("Duplicate value not allowed");
    }
    std::string next_leaf_ptr;
    bool leaf_ptr = false;

    // Check if the record_ids vector is not empty
    if (!record_ids.empty()) {
        // Get the last element
        next_leaf_ptr = record_ids.back();
        std::cout << "back id: " << next_leaf_ptr << "\n";

        // Check if the last value is really a leaf pointer and not a record ID
        if (next_leaf_ptr.find('-') == std::string::npos) {
            // Remove the pointer as it is a leaf pointer
            record_ids.pop_back();
            leaf_ptr = true;
        }
    }


    // Find the correct position to insert
    auto it = std::lower_bound(values.begin(), values.end(), attribute);
    int index = it - values.begin();

            record_ids.push_back(record_id);
        }
    }*/



    //check if overflow
    if(values.size() >= BPTreeNode::MAX_VALUES){

        // Create a new leaf
        std::string new_leaf_id = buffer_manager->create_new_block();
        std::shared_ptr<BPTreeNode> new_leaf = BPTreeNode::create_node(buffer_manager, new_leaf_id, get_parent_id(), true);

        std::cout << "new leaf id: " << new_leaf_id << "\n";
        //median
        //+1
        int median_index = (values.size())/2;
        int median = values[median_index];

        // Move half of the values and record IDs to the new node
        // new leaf should not contain the median => +1
        std::vector<int> new_values(values.begin() + median_index+1, values.end());
        std::vector<std::string> new_record_ids(record_ids.begin() + median_index+1, record_ids.end());

        if(leaf_ptr){
            //if pointer exists, add to vector
            new_record_ids.push_back(next_leaf_ptr);
            std::cout << "There is a leaf id \n";
            std::cout << next_leaf_ptr << "\n";
        }

        std::cout << record_ids.size() << " " << values.size() << "\n";
        for (const auto& item : values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;

        // Resize and update the original node
        values.resize(median_index+1);
        record_ids.resize(median_index+1);
        record_ids.push_back(new_leaf_id); // Point to the new leaf

         // Update next leaf pointer in the original node
        //if (record_ids.size() == (values.size()+1)){
        //    record_ids[record_ids.size() - 1] = new_leaf_id;
        //}else{

        //}


        //Save changes in original node
        change_values(values);
        change_children_ids(record_ids);

        //Save new leaf
        new_leaf->change_values(new_values);
        new_leaf->change_children_ids(new_record_ids);

        std::cout << "\n MEDIAN: " << median << "\n \n";

        //propagation is done later
        std::cout << record_ids.size() << " " << values.size() << "\n";
        for (const auto& item : values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        std::cout << new_record_ids.size() << " " << new_values.size() << "\n";
        for (const auto& item : new_values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : new_record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        // Return the new node and the median
        return std::make_pair(new_leaf, median);
    }

    if(leaf_ptr){
        //if pointer exists, add to vector
        record_ids.push_back(next_leaf_ptr);
        //std::cout <<
    }

    // Update the node with new values and record IDs
    change_values(values);
    change_children_ids(record_ids);

    return std::nullopt;
}

std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> BPTreeNode::insert_value(int attribute, std::string const& left_children_id, std::string const& right_children_id)
{
    //get present values and record ids
    std::vector<int> values = get_values();
    std::vector<std::string> children_ids = get_children_ids();

    // Find the correct position to insert
    auto it = std::lower_bound(values.begin(), values.end(), attribute);
    int index = it - values.begin();

       // Insert the attribute and record ID at the found position
    values.insert(it, attribute);
    // Check if children_ids is empty and handle the first insertion
    if (children_ids.empty()) {
        // This is the first insertion into this internal node
        children_ids.push_back(left_children_id); // Insert left child at the beginning
        children_ids.push_back(right_children_id); // Insert right child next
    } else {
        // For subsequent insertions, insert the right child ID at the correct position
        children_ids.insert(children_ids.begin() + index + 1, right_children_id);
    }

    for (const auto& item : values) {
    std::cout << item << " ";
    }
    std::cout << std::endl;
    for (const auto& item : children_ids) {
    std::cout << item << " ";
    }
    std::cout << std::endl;

    // Update the node with new values and children IDs
    change_values(values);
    change_children_ids(children_ids);

    //check if overflow
    if(values.size() >= BPTreeNode::MAX_VALUES){
        // Create a new node
        std::string new_node_id = buffer_manager->create_new_block();
        std::shared_ptr<BPTreeNode> new_node = BPTreeNode::create_node(buffer_manager, new_node_id, get_parent_id(), false);

        //median
        //excluding in both nodes
        //push to parent
        int median_index = values.size() / 2;
        int median = values[median_index];

        // Move the second half of the values and children to the new node
        std::vector<int> new_values(values.begin() + median_index + 1, values.end());
        std::vector<std::string> new_children_ids(children_ids.begin() + median_index + 1, children_ids.end());
        //resize original node
        values.resize(median_index);
        children_ids.resize(median_index+1);  // Keeping the extra child ID

        // get parent
        //std::shared_ptr<BPTreeNode> parent = std::make_shared<BPTreeNode>(buffer_manager, get_parent_id());
        //parent->insert_value(median, get_node_id(), new_node_id);

        /*if (parent->get_node_id() == "-----") {
            // The current node is the root, create a new root and update the tree root reference
        std::string new_root_id = buffer_manager->create_new_block();
        auto new_root = BPTreeNode::create_node(buffer_manager, new_root_id, "-----", false);
        new_root->insert_value(median, get_node_id(), new_node_id);
        //root_node_id = new_root_id; // Update the tree's root node ID
        } else {
            // Insert median into the parent node
            auto result = parent->insert_value(median, get_node_id(), new_node_id);

            if (result) {
                // If parent overflows, handle the overflow (potentially requires recursive logic or a loop)
                // ... Handle parent overflow ...
            }
        }*/

        change_values(values);
        change_children_ids(children_ids);

        new_node->change_values(new_values);
        new_node->change_children_ids(new_children_ids);

        //propagation is done later
        /*
            for (const auto& item : values) {
            std::cout << item << " ";
            }
            std::cout << std::endl;
            for (const auto& item : children_ids) {
            std::cout << item << " ";
            }
            std::cout << std::endl;

            for (const auto& item : new_values) {
            std::cout << item << " ";
            }
            std::cout << std::endl;
            for (const auto& item : new_children_ids) {
            std::cout << item << " ";
            }
            std::cout << std::endl;
        */
        // Return the new node and the median
        return std::make_pair(new_node, median);
    }
    return std::nullopt;
}


BPTree::BPTree(std::shared_ptr<BufferManager> const& buffer_manager, std::string const& root_node_id)
: buffer_manager(buffer_manager), root_node_id(root_node_id)
{
    if (!buffer_manager->block_exists(root_node_id))
        BPTreeNode::create_node(buffer_manager, root_node_id, NO_PARENT, true);
}

std::optional<std::string> BPTree::search_record(int attribute)
{
    // find correct leaf node
    std::shared_ptr<BPTreeNode> leaf_node = find_leaf_node(attribute);

    std::vector<int> values = leaf_node->get_values();
    // children IDs are record IDs in leaf nodes
    std::vector<std::string> record_ids = leaf_node->get_children_ids();

    std::cout << "attr: " << attribute << "\n";
    for (const auto& item : values) {
        std::cout << item << " ";
    }
    std::cout << std::endl;

    for (const auto& item : record_ids) {
        std::cout << item << " ";
    }
    std::cout << std::endl;

    // search for attribute
    for (int i = 0; i < values.size(); i++)
    {
        if (values.at(i) == attribute)
            return record_ids.at(i);
    }

    // not found
    return std::nullopt;
}

bool BPTree::insert_record(int attribute, std::string const& record_id)
{
    // find correct leaf node
    std::shared_ptr<BPTreeNode> leaf_node = find_leaf_node(attribute);

    // insert record in the leaf node (if enough space is present)
    auto result = leaf_node->insert_record(attribute, record_id);

    if (!result.has_value())
        // no overflow, done
        return true;

    std::shared_ptr<BPTreeNode> new_node = result->first;
    int median = result->second;

    // check BPTree property
    int val_diff = leaf_node->get_values().size() - new_node->get_values().size();
    assert(0 <= val_diff && val_diff <= 1);
    assert(leaf_node->get_children_ids().size() - leaf_node->get_values().size() == 1);

    if (new_node == nullptr)
        return false;

    // insert median into parent node
    std::shared_ptr<BPTreeNode> current = leaf_node;
    std::shared_ptr<BPTreeNode> child = new_node;

    while (true) {
        // check if current is root
        if (current->get_parent_id() == NO_PARENT) {
            // create new root
            std::string new_root_id = buffer_manager->create_new_block();
            std::shared_ptr<BPTreeNode> new_root = BPTreeNode::create_node(buffer_manager, new_root_id, NO_PARENT, false);

            // change parent ids
            assert(current->change_parent_id(new_root->get_node_id()) && child->change_parent_id(new_root->get_node_id()));

            // Insert median and pointers into new root
            new_root->insert_value(median, current->get_node_id(), child->get_node_id());

            // check BPTree property
            val_diff = current->get_values().size() - child->get_values().size();
            assert(0 <= val_diff && val_diff <= 1);
            assert(current->get_children_ids().size() - current->get_values().size() == 1);

            // update root_node_id
            root_node_id = new_root_id;
            return true;
        }

        // check BPTree property
        val_diff = current->get_values().size() - child->get_values().size();
        assert(0 <= val_diff && val_diff <= 1);
        assert(current->get_children_ids().size() - current->get_values().size() == 1);

        // get parent
        std::shared_ptr<BPTreeNode> parent = std::make_shared<BPTreeNode>(buffer_manager, current->get_parent_id());

        // change parent ids
        assert(current->change_parent_id(parent->get_node_id()) && child->change_parent_id(parent->get_node_id()));

        // insert median and pointers into parent
        result = parent->insert_value(median, current->get_node_id(), child->get_node_id());

        if (!result.has_value()) {
            // no overflow, done
            return true;
        }

        // split parent if it overflows and propagate upwards
        current = parent;
        child = result->first;
        median = result->second;
    }
    return true;
}

std::string BPTree::get_root_node_id()
{
    return root_node_id;
}

std::shared_ptr<BPTreeNode> BPTree::find_leaf_node(int attribute)
{
    // get root node
    std::shared_ptr<BPTreeNode> current_node = std::make_shared<BPTreeNode>(buffer_manager, root_node_id);

    // navigate to correct leaf node
    while (!current_node->is_leaf())
    {
        std::vector<int> values = current_node->get_values();
        std::vector<std::string> children_ids = current_node->get_children_ids();

        std::string next_node_id;

        // check values and traverse
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (attribute < values.at(i))
            {
                next_node_id = children_ids.at(i);
                break;
            }

            // edge case: last pointer
            if (i == values.size() - 1)
            {
                next_node_id = children_ids.at(i+1);
                break;
            }
        }

        // update current node
        current_node = std::make_shared<BPTreeNode>(buffer_manager, next_node_id);
    }

    return current_node;
}