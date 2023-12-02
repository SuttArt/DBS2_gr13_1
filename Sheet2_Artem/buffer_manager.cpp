// -------------------------------
// Group:
// Members:
// HU-Account names:
// -------------------------------
#include <memory>
#include <string>

#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"

BufferManager::BufferManager(int n_blocks) : n_blocks(n_blocks) {}

std::shared_ptr<Block> BufferManager::fix_block(std::string const& block_id)
{
    // Implement your solution here
    return nullptr;
}

bool BufferManager::unfix_block(std::string const& block_id)
{
    // Implement your solution here
    return false;
}