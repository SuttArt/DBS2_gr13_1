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
#include <iostream>
#include <set>
#include <algorithm>

#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"
#include "header/bptree.h"


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

bool hasDuplicates(const std::vector<int>& numbers) {
    std::set<int> uniqueNumbers;

    for (int num : numbers) {
        // Try to insert the current number into the set
        auto result = uniqueNumbers.insert(num);

        // If the insertion fails, it means the number is already present (duplicate)
        if (!result.second) {
            return true; // Duplicate found
        }
    }

    return false; // No duplicates found
}
std::pair<int, int> findMedian(const std::vector<int>& numbers) {
    // Create a copy of the vector to avoid modifying the original
    std::vector<int> sortedNumbers = numbers;

    // Calculate the index of the middle element
    size_t size = sortedNumbers.size();
    size_t middleIndex = size / 2;

    // Check if the number of elements is odd or even
    if (size % 2 == 1) {
        // Odd number of elements, return the middle element
        middleIndex + 1;
    }

    // Even number of elements, return the average of the two middle elements
    return std::make_pair(sortedNumbers[middleIndex], middleIndex);

}

std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> BPTreeNode::insert_record(int attribute, std::string const& record_id)
{
    std::vector<int> values = get_values();
    std::vector<std::string> children = get_children_ids();

    // Merge two vectors to keep data together
    std::vector<std::pair<int, std::string>> values_ids;

    // take data from node
    for (size_t i = 0; i < values.size(); ++i) {
        values_ids.push_back(std::make_pair(values[i], children[i]));
    }

    // if we have last pointer without value
    std::string last_child;
    if (!values.empty() && (children.size() - values.size()) == 1) {
        last_child = children.back();
    }

    // add new values
    values_ids.push_back(std::make_pair(attribute, record_id));

    std::sort(values_ids.begin(), values_ids.end());

    // clear the vectors to make them available
    values.clear();
    children.clear();

    // split vectors
    for (const auto& pair : values_ids) {
        values.push_back(pair.first);
        children.push_back(pair.second);
    }

    if (hasDuplicates(values))
        throw std::invalid_argument("Cannot have duplicate values in index block: " + block_id);

    // if we don't have to split node
    if (values.size() <= MAX_VALUES) {

        if (!last_child.empty())
            children.push_back(last_child);

        assert(change_values(values));
        assert(change_children_ids(children));

        return std::nullopt;
    }

    std::pair<int, int> median_pair = findMedian(values);
    int median = median_pair.first;
    int median_index = median_pair.second;

    // if we have to split node

    // Create two separate vectors based on the median_index for values and children

    // adjust original leaf node values
    std::vector<int> original_leaf_node_values(values.begin(),values.begin() + median_index);
    std::vector<std::string> original_leaf_node_children(children.begin(),children.begin() + median_index);


    // create new leaf node values
    std::vector<int> new_leaf_node_values(values.begin() + median_index, values.end());
    std::vector<std::string> new_leaf_node_children(children.begin() + median_index, children.end());

    // create new leaf node
    std::string node_id_new_leaf_node = buffer_manager->create_new_block();

    std::shared_ptr<BPTreeNode> new_leaf_node = create_node(buffer_manager, node_id_new_leaf_node, get_parent_id(), true);

    // add pointer for new block to next block
    if (!last_child.empty())
        new_leaf_node_children.push_back(last_child);

    // write new data to original leaf node
    assert(new_leaf_node->change_values(new_leaf_node_values));
    assert(new_leaf_node->change_children_ids(new_leaf_node_children));

    // add pointer to new block for original leaf node
    original_leaf_node_children.push_back(new_leaf_node->get_node_id());
    // write new data to original leaf node
    assert(change_values(original_leaf_node_values));
    assert(change_children_ids(original_leaf_node_children));


    return std::make_pair(new_leaf_node, median);
}

std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> BPTreeNode::insert_value(int attribute, std::string const& left_children_id, std::string const& right_children_id)
{
    std::vector<int> values = get_values();
    std::vector<std::string> children = get_children_ids();

    // Use lower_bound to find the position where attribute should be inserted
    auto it_values = std::lower_bound(values.begin(), values.end(), attribute);
    // Calculate and return the index of the inserted value
    int newIndex = std::distance(values.begin(), it_values);
    // Insert the new value at the found position
    values.insert(it_values, attribute);

    if (children.empty()){
        children.insert(children.begin() + newIndex, left_children_id);
    } else {
        children.at(newIndex) = left_children_id;
    }
    // add left_children_id and right_children_id
    children.insert(children.begin() + newIndex+1, right_children_id);

    // if we don't have to split node
    if (values.size() <= MAX_VALUES) {

        assert(change_values(values));
        assert(change_children_ids(children));

        return std::nullopt;
    }

    // if we have to split node
    std::pair<int, int> median_pair = findMedian(values);
    int median = median_pair.first;
    int median_index = median_pair.second;

    // Create two separate vectors based on the median_index for values and children

    // adjust original internal node values
    std::vector<int> original_internal_node_values(values.begin(), values.begin() + median_index);
    std::vector<std::string> original_internal_node_children(children.begin(), children.begin() + median_index + 1);


    // create new internal node values
    std::vector<int> new_internal_node_values(values.begin() + median_index + 1, values.end());
    std::vector<std::string> new_internal_node_children(children.begin() + median_index + 1, children.end());

    // create new internal node
    std::string node_id_new_internal_node = buffer_manager->create_new_block();

    std::shared_ptr<BPTreeNode> new_internal_node = create_node(buffer_manager, node_id_new_internal_node, get_parent_id(), false);

    // write new data to original internal node
    assert(new_internal_node->change_values(new_internal_node_values));
    assert(new_internal_node->change_children_ids(new_internal_node_children));

    // write new data to original internal node
    assert(change_values(original_internal_node_values));
    assert(change_children_ids(original_internal_node_children));

    // adapt parent node references
    for (size_t i = 0; i < new_internal_node_children.size(); ++i) {
        std::shared_ptr<BPTreeNode> child_node = std::make_shared<BPTreeNode>(buffer_manager, new_internal_node_children[i]);
        child_node->change_parent_id(new_internal_node->get_node_id());
    }

    return std::make_pair(new_internal_node, median);

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
    std::string leaf_node_parent = leaf_node->get_parent_id();

    std::vector<int> values = leaf_node->get_values();
    // children IDs are record IDs in leaf nodes
    std::vector<std::string> record_ids = leaf_node->get_children_ids();

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