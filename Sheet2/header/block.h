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
#ifndef TASK_3_BLOCK_H
#define TASK_3_BLOCK_H

#include <memory>
#include <string>

#include "record.h"

class Block
{
public:
    Block(std::string const& block_id);

    std::string get_block_id();

    std::shared_ptr<Record> add_record(std::vector<Record::Attribute> const& attributes);

    std::shared_ptr<Record> get_record(std::string const& record_id);

    bool delete_record(std::string const& record_id);

    bool is_dirty();

    bool write_data();

    static std::string create_block_id(int offset);

    static std::string get_block_id(std::string const& record_id);

    static int get_block_dictionary_offset(std::string const& record_id);

    static int const BLOCK_ID_SIZE = 5;
    static int const BLOCK_SIZE = 4096;
    std::string const BLOCK_DIR = "data/";
    static int const MAX_RECORDS = 64;

private:
    std::shared_ptr<void> load_data(std::string const& block_id);

    std::shared_ptr<void> data;
    bool dirty;
};

#endif
