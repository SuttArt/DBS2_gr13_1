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
#include <memory>
#include <string>

#include "header/record.h"
#include "header/block.h"
#include "header/buffer_manager.h"

//added:
#include <unordered_map>
#include <queue>
#include <iostream>

BufferManager::BufferManager(int n_blocks) : n_blocks(n_blocks) {}

//define the structures
std::unordered_map<std::string,int> fixCount;                               //ID, counter
std::unordered_map<std::string, std::shared_ptr<Block>> cache;              //ID, content
std::queue<std::string> unfixed;

//retrieve block by its id from cache/local file
//registers fix
//when not in cache: emplaced init
//if full: replace via LRU
//when not possible: runtime error
std::shared_ptr<Block> BufferManager::fix_block(std::string const& block_id)
{
    if(cache.find(block_id) != cache.end()){                                //proof if block is already in cache
        fixCount[block_id]++;                                               //yes: increase counter
        return std::make_shared<Block>(block_id);                           //return added block
    }
    else{                                                                   //no: insert
        if (cache.size() < n_blocks){                                       //cache not full
            fixCount.emplace(block_id,1 );                                  //insert
            //std::make_shared<Block>(block_id): get data/content of block
            cache.emplace(block_id,std::make_shared<Block>(block_id));      //insert block in cache

            return std::make_shared<Block>(block_id);                       //return added block
        }
        else{                                                               //LRU
            if(!unfixed.empty()){                           //proof if block with count 0 exists
                std::string unused_block = unfixed.front(); //get oldest block of unused blocks to replace
                unfixed.pop();                              //remove block ID from queue

                cache.erase(unused_block);                  //delete block from cache
                fixCount.erase(unused_block);               //remove block in Counting Structure
                //Insert new block in both structures
                fixCount.emplace(block_id,1 );
                cache.emplace(block_id,std::make_shared<Block>(block_id));

                return std::make_shared<Block>(block_id);   //return added block
            }else{
                throw std::runtime_error("All blocks are fixed at the moment");
            }
        }
    }
}

//unfixes block by id
//if block not present or can not be further unfixed: runtime error
bool BufferManager::unfix_block(std::string const& block_id)
{
    //Error handling: Block must be there and cannot be unfixed further than 0
    if(cache.find(block_id) == cache.end() || fixCount[block_id] <= 0 ){
        throw std::runtime_error("Could not find block in cache or block can not be further unfixed");
    }else{                                                          //unfix block
        fixCount[block_id]--;                                       //decrease fix counter
        if(fixCount[block_id] == 0){                                //when block is unused
            unfixed.push(block_id);                                 //add to queue for possible replacements
        }
        return true;                                                //unfix was successful
    }
}