#ifndef TASK_3_BUFFER_MANAGER_H
#define TASK_3_BUFFER_MANAGER_H

#include <memory>
#include <string>
#include <list>
#include <unordered_map>

#include "record.h"
#include "block.h"

class BufferManager
{
public:
    BufferManager(int n_blocks);

    std::shared_ptr<Block> fix_block(std::string const& block_id);

    bool unfix_block(std::string const& block_id);

    bool block_exists(std::string const& block_id);

    std::string create_new_block();

private:
    int n_blocks;

    struct CacheEntry {
        std::shared_ptr<Block> block;
        int reference_count;

        CacheEntry() : block(nullptr), reference_count(0) {}
        CacheEntry(std::shared_ptr<Block> const& block, int ref_count) : block(block), reference_count(ref_count) {}
    };

    // Map a block ID to a CacheEntry
    std::unordered_map<std::string, CacheEntry> cache;

    // Keeps track of blocks that are not fixed, for eviction purposes
    std::list<std::string> unfixed_blocks;

    // Contains meta information
    static std::string const BLOCK_ID;
};

#endif
