// -------------------------------
// Group: 13
// Members:
//      Charlotte Kauter
//      Robert Sitner
//      Artem Suttar
//HU-Account names:
//      kauterch
//      sitnerro
//      suttarar
// -------------------------------
#ifndef TASK_3_BUFFER_MANAGER_H
#define TASK_3_BUFFER_MANAGER_H

#include <memory>
#include <string>

#include "record.h"
#include "block.h"

class BufferManager
{
public:
    BufferManager(int n_blocks);

    std::shared_ptr<Block> fix_block(std::string const& block_id);

    bool unfix_block(std::string const& block_id);

private:
    int n_blocks;
};

#endif
