#include <memory>
#include <string>
#include <list>
#include <unordered_map>
#include <algorithm>

#include "header/filesystem.h"
#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"

 std::string const BufferManager::BLOCK_ID = "bfmgr";

BufferManager::BufferManager(int n_blocks) : n_blocks(n_blocks)
{
    if (block_exists(BLOCK_ID))
        return;

    std::shared_ptr<Block> block = fix_block(BLOCK_ID);

    if (block == nullptr)
        throw std::invalid_argument("Cannot load buffer manager block: " + BLOCK_ID);

    // add amount of created blocks
    std::shared_ptr<Record> r = block->add_record({(int) 0});

    if (r == nullptr)
        throw std::invalid_argument("Cannot add number of created blocks in " + BLOCK_ID);

    unfix_block(BLOCK_ID);
}

std::shared_ptr<Block> BufferManager::fix_block(std::string const& block_id)
{
    // Check if the block is already in cache
    auto it = cache.find(block_id);

    if (it != cache.end()) {
        // Increase the reference count
        it->second.reference_count++;

        // Note that in 'least recently unfixed' strategy, we do not move the block in the LRU list here
        return it->second.block;
    }

    // If cache is full and no block can be evicted, return nullptr
    if (cache.size() >= n_blocks && unfixed_blocks.empty()) {
        throw std::runtime_error("Cannot fix block. Cache is already full.");
    }

    // Evict the least recently unfixed block if necessary
    if (cache.size() >= n_blocks) {
        // Get the ID of the least recently unfixed block
        std::string block_id_to_evict = unfixed_blocks.front();
        unfixed_blocks.pop_front();

        // Evict the block and write to disk if it's dirty
        std::shared_ptr<Block> block_to_evict = cache[block_id_to_evict].block;
        if (block_to_evict && block_to_evict->is_dirty()) {
            block_to_evict->write_data();
        }

        // Remove from the cache
        cache.erase(block_id_to_evict);
    }

    // Load the block into cache
    std::shared_ptr<Block> block = std::make_shared<Block>(block_id);
    // Set reference count to 1
    cache[block_id] = {block, 1};

    // Do not add to unfixed_blocks list because it's being fixed for the first time
    return block;
}

bool BufferManager::unfix_block(std::string const& block_id)
{
    auto it = cache.find(block_id);

    if (it == cache.end())
        throw std::invalid_argument("Cannot unfix block that is not in cache.");

    if (it->second.reference_count == 0)
        throw std::invalid_argument("Cannot unfix a block without fixes.");

    // Decrease the reference count
    it->second.reference_count--;

    // If the reference count is zero, add to the list of unfixed blocks
    if (it->second.reference_count == 0 && std::find(unfixed_blocks.begin(), unfixed_blocks.end(), block_id) == unfixed_blocks.end()) {
        unfixed_blocks.push_back(block_id);
    }

    return true;
}

bool BufferManager::block_exists(const std::string &block_id)
{
    // Check if the block exists in cache
    auto it = cache.find(block_id);

    if (it != cache.end())
        return true;

    // Otherwise, check if it exists on disk
    return std::filesystem::exists(Block::BLOCK_DIR + block_id);
}

std::string BufferManager::create_new_block()
{
    std::shared_ptr<Block> block = fix_block(BLOCK_ID);

    if (block == nullptr)
        throw std::runtime_error("Cannot load buffer manager block: " + BLOCK_ID);

    // get number of existing blocks
    int n_blocks = block->get_record(Block::create_record_id(BLOCK_ID, 0))->get_integer_attribute(1);

    // increment and save new amount
    std::shared_ptr<Record> record = std::make_shared<Record>(Record(Block::create_record_id(BLOCK_ID, 0), {(int) ++n_blocks}));

    if (!block->update_record(record))
        throw std::runtime_error("Cannot save amount of blocks in: " + BLOCK_ID);

    unfix_block(BLOCK_ID);
    return Block::create_block_id(n_blocks);
}