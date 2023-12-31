// -------------------------------
// Group:
// Members:
// HU-Account names:
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

//ADDED:
#include <stdexcept> // got the following error: std has no member 'invalid_argument'
#include <algorithm>
#include <iostream>

#include <iomanip>
#include <sstream>

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
    //read all attributes and record ids
    std::vector<int> attributes = get_values();
    std::vector<std::string> record_ids = get_children_ids();
    /*
    for (const auto& item : record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
    */

    //proof if attribute is a duplicate
    if(std::find(attributes.begin(),attributes.end(), attribute) != attributes.end()){
        std::invalid_argument("Duplicates can not be inserted!");
    }

    //1. insert attribute
    //1.1 find correct position
    auto it = std::lower_bound(attributes.begin(), attributes.end(), attribute); //returns point to insert
    int index = it - attributes.begin();
    //1.2 insert both values
    attributes.insert(it, attribute);
    record_ids.insert(record_ids.begin() + index, record_id);

    //2. Proof if pointer exists
    std::string ptr = record_ids.back();
    bool ptr_exists = false;
    //if it does not contain '-', it's a leaf id
    //-----
    if(ptr.find("-----") == std::string::npos){
        ptr_exists = true;
        record_ids.pop_back(); //remove ptr
        //std::cout << "tmp ptr: " << ptr << "\n";
    }

    if(attribute > 5800 && attribute < 5900){
        for (const auto& item : attributes) {
        std::cout << item << " ";
         }
        std::cout << std::endl;
        for (const auto& item : record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        std::cout << std::endl;

        if(std::find(record_ids.begin(),record_ids.end(),"-----05816") == record_ids.end()&& attributes.back() > 5816){
            //std::__throw_logic_error("ERROR!");
        }
        //std::__throw_logic_error("ERROR!");
    }

    /*for (const auto& item : attributes) {
        std::cout << item << " ";
    }
        std::cout << std::endl;
    for (const auto& item : record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;

    std::stringstream id;
    id << "-----"<< std::setw(5) << std::setfill('0') << attribute;
        if(id.str() != record_id){
            std::__throw_logic_error("ERROR!");
        }

    if(attributes.size() >2 && index > 1){
        std::stringstream ss;
        ss << "-----"<< std::setw(5) << std::setfill('0') << attributes[index-1];
        if(ss.str() != record_ids[index-1]){
            std::cout <<"attribute: " << attribute << "\n";
            std::cout << "index: " << index << ", " << index-1 << "\n";
            std::cout << "before attr: " << attributes[index-1] << "\n";
            std::cout << "id: " << record_ids[index-1] << "\n";
            std::__throw_logic_error("ERrOR!");
        }}*//*
        if(("-----"+ attributes[index+1]) != record_ids[index+1]){
            std::cout << attribute << "\n";
            std::cout << attributes[index+1] << "\n";
            std::cout << record_ids[index+1] << "\n";
            std::__throw_logic_error("ERrOR!");
        }
    }
    /*
    for (const auto& item : attributes) {
    std::cout << item << " ";
    }
    std::cout << std::endl;
    */

    //3. proof if leaf overflows
    //YES:
    if(attributes.size() >= BPTreeNode::MAX_VALUES){
        //3.1 create new id and leaf
        std::string new_leaf_id = buffer_manager->create_new_block();
        std::shared_ptr<BPTreeNode> new_leaf = BPTreeNode::create_node(buffer_manager, new_leaf_id, get_parent_id(), true);
        //3.2 calculate median
        int median_index = (attributes.size())/2;
        int median = attributes[median_index];
        //3.3 fill new leaf
        std::vector<int> new_attributes(attributes.begin() + median_index+1, attributes.end()); //median have to be on the left side
        //std::cout <<"last id:" << record_ids[record_ids.size()-1]<< "\n";
        std::vector<std::string> new_record_ids(record_ids.begin() + median_index+1, record_ids.end());
        //3.3.1 if there was a ptr in the original leaf, add it
        if(ptr_exists){
            new_record_ids.push_back(ptr);
        }
        /*
        for (const auto& item : attributes) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : new_attributes) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
         for (const auto& item : new_record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;

        if(attributes.size() >2 && index > 1){
        std::stringstream ss;
        ss << "-----"<< std::setw(5) << std::setfill('0') << attributes[index-1];
        if(ss.str() != record_ids[index-1]){
            std::cout <<"attribute: " << attribute << "\n";
            std::cout << "index: " << index << ", " << index-1 << "\n";
            std::cout << "before attr: " << attributes[index-1] << "\n";
            std::cout << "id: " << record_ids[index-1] << "\n";
            std::__throw_logic_error("ERrOR!");
        }
        }*/

        //3.4 resize original leaf
        attributes.resize(median_index+1); //input is a size => +1
        record_ids.resize(median_index+1);
        //3.4.1 add ptr to new leaf
        record_ids.push_back(new_leaf_id);
        //std::cout << "new ptr: " << new_leaf_id << "\n";

        //3.5 save all changes
        //3.5.1 old leaf
        assert(change_values(attributes));
        assert(change_children_ids(record_ids));
        //3.5.2 new leaf
        assert(new_leaf -> change_values(new_attributes));
        assert(new_leaf -> change_children_ids(new_record_ids));
        /*
        for (const auto& item : attributes) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : new_attributes) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
         for (const auto& item : new_record_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        */
        return std::make_pair(new_leaf, median);
    //NO:
    }else{
        //if there was a pointer, write it back
        if(ptr_exists){
            record_ids.push_back(ptr);
        }
        //save changes
        assert(change_values(attributes));
        assert(change_children_ids(record_ids));
        return std::nullopt;
    }
}

std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> BPTreeNode::insert_value(int attribute, std::string const& left_children_id, std::string const& right_children_id)
{
    //read all attributes and record ids
    std::vector<int> values = get_values();
    std::vector<std::string> children_ids = get_children_ids();
    /*std::cout << "before insert: \n";
    for (const auto& item : values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        for (const auto& item : children_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
    */
    //1. insert attribute
    //1.1 find correct position
    auto it = std::lower_bound(values.begin(), values.end(), attribute); //returns point to insert
    int index = it - values.begin();
    //1.2 insert attribute
    values.insert(it, attribute);
    //1.3 insert ids
    if(children_ids.empty()){
        //1.3.1 insert both ids
        children_ids.push_back(left_children_id);
        children_ids.push_back(right_children_id);
    }else{
        //1.3.2 for subsequent insertions, insert the right child ID at the correct position
        children_ids.insert(children_ids.begin() + index +1, right_children_id);
    }

    /*
    std::cout << "left: " << left_children_id << "\nright: " << right_children_id << "\n";
    std::cout << "after insert: \n";
    std::cout << "values: \n";
    for (const auto& item : values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
    std::cout << "ids: \n";
        for (const auto& item : children_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
    */
    //2. proof if internal node overflows
    //YES:
    if(values.size() >= BPTreeNode::MAX_VALUES){
        //2.1 create a new node and id
        std::string new_node_id = buffer_manager->create_new_block();
        std::shared_ptr<BPTreeNode> new_node = BPTreeNode::create_node(buffer_manager, new_node_id, get_parent_id(), false);
        //2.2 calculate median
        int median_index = (values.size())/ 2;
        int median = values[median_index];
        //2.3 fill new node
        std::vector<int> new_values(values.begin() + median_index +1, values.end()); //median have to be on the left side
        //std::cout << children_ids[children_ids.size()-1]<< "\n";
        std::vector<std::string> new_children_ids(children_ids.begin() + median_index +1, children_ids.end());
        //2.4 resize original node
        values.resize(median_index); //exclude median
        children_ids.resize(median_index+1); //but keep id of median
        //2.5 add median to parent????


        //2.6 save all changes
        //2.6.1 old node
        assert(change_values(values));
        assert(change_children_ids(children_ids));
        //2.6.2 new node
        assert(new_node -> change_values(new_values));
        assert(new_node -> change_children_ids(new_children_ids));
        /*std::cout << " split origin: \n";
        std::cout << "values: \n";
        for (const auto& item : values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        std::cout << "ids: \n";
        for (const auto& item : children_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        std::cout << "new split: \n";
        std::cout << "values: \n";
        for (const auto& item : new_values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        std::cout << "ids: \n";
        for (const auto& item : new_children_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        */

        return std::make_pair(new_node, median);
    //NO:
    }else{
        //save changes
        assert(change_values(values));
        assert(change_children_ids(children_ids));

        return std::nullopt;
    }
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
    /*
    std::cout << "attr: " << attribute << "\n";
    for (const auto& item : values) {
        std::cout << item << " ";
    }
    std::cout << std::endl;

    for (const auto& item : record_ids) {
        std::cout << item << " ";
    }
    std::cout << std::endl;
    */
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

    int internal = 0;

    // insert record in the leaf node (if enough space is present)
    auto result = leaf_node->insert_record(attribute, record_id);

    //std::cout << "leaf node id------------------ " << leaf_node->get_node_id() << "\n";

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
        internal++;
        //std::cout << "Internal: " << internal << "\n";

        if (!result.has_value()) {
            // no overflow, done
            return true;
        }

        // split parent if it overflows and propagate upwards
        current = parent;
        /*
        std::vector<int> n_values = current->get_values();
        std::vector<std::string> n_ids = current->get_children_ids();
        std::cout << "parent ---- \n";
        std::cout << "values: \n";
        for (const auto& item : n_values) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        std::cout << "ids: \n";
        for (const auto& item :n_ids) {
        std::cout << item << " ";
        }
        std::cout << std::endl;
        */
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