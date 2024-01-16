#ifndef TASK_3_BPTREE_H
#define TASK_3_BPTREE_H

#include <memory>
#include <string>
#include <optional>

#include "block.h"
#include "buffer_manager.h"

class BPTreeNode
{
public:
    BPTreeNode(std::shared_ptr<BufferManager> const& buffer_manager, std::string const& node_id);

    std::string get_node_id();

    std::string get_parent_id();

    bool is_leaf();

    std::vector<int> get_values();

    std::vector<std::string> get_children_ids();

    bool change_parent_id(std::string parent_id);

    bool change_values(std::vector<int> const& values);

    bool change_children_ids(std::vector<std::string> const& children_ids);

    std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> insert_record(int attribute, std::string const& record_id);

    std::optional<std::pair<std::shared_ptr<BPTreeNode>, int>> insert_value(int attribute, std::string const& left_children_id, std::string const& right_children_id);

    static std::shared_ptr<BPTreeNode> create_node(std::shared_ptr<BufferManager> const& buffer_manager, std::string const& node_id, std::string parent_id, bool leaf);

    static int const MAX_VALUES = 29;
    static int const MAX_CHILDREN = 30;

private:
    std::shared_ptr<BufferManager> buffer_manager;
    std::string block_id;
};

class BPTree
{
public:
    BPTree(std::shared_ptr<BufferManager> const& buffer_manager, std::string const& root_node_id);

    std::optional<std::string> search_record(int attribute);

    bool insert_record(int attribute, std::string const& record_id);

    std::string get_root_node_id();

    bool erase();

private:
    std::shared_ptr<BPTreeNode> find_leaf_node(int attribute);

    std::shared_ptr<BufferManager> buffer_manager;
    std::string root_node_id;

    std::string const NO_PARENT = "-----";
};

#endif