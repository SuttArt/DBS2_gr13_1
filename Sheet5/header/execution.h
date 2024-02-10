#ifndef TASK_3_EXECUTION_H
#define TASK_3_EXECUTION_H

#include <memory>
#include <string>
#include <vector>

#include "record.h"
#include "block.h"
#include "bptree.h"
#include "buffer_manager.h"

class QueryOperator
{
public:
    virtual bool open() {return true;}

    virtual std::shared_ptr<Record> next() {return nullptr;}

    virtual bool close() {return true;}
};


class Table : public QueryOperator
{
public:
    Table(std::shared_ptr<BufferManager> const& buffer_manager, std::vector<std::string> const& block_ids);

    virtual bool open() override;

    virtual std::shared_ptr<Record> next() override;

    virtual bool close() override;

private:
    std::shared_ptr<BufferManager> buffer_manager;
    std::vector<std::string> block_ids;

    int current_block;
    int current_record;
};

class Projection : public QueryOperator
{
public:
    Projection(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source,
               std::vector<int> const& positions, std::vector<std::string> const& attribute_types);

    virtual bool open() override;

    virtual std::shared_ptr<Record> next() override;

    virtual bool close() override;

private:
    std::shared_ptr<BufferManager> buffer_manager;
    std::shared_ptr<QueryOperator> source;
    std::vector<int> positions;
    std::vector<std::string> attribute_types;
};

class Selection : public QueryOperator
{
public:
    Selection(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source,
               int attribute_position, std::string const& attribute_type, Record::Attribute const& value, std::string const& comparator);

    virtual bool open() override;

    virtual std::shared_ptr<Record> next() override;

    virtual bool close() override;

private:
    std::shared_ptr<BufferManager> buffer_manager;
    std::shared_ptr<QueryOperator> source;
    int attribute_position;
    std::string attribute_type;
    Record::Attribute value;
    std::string comparator;
};

class Distinct : public QueryOperator
{
public:
    Distinct(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source);

    virtual bool open() override;

    virtual std::shared_ptr<Record> next() override;

    virtual bool close() override;

private:
    std::shared_ptr<BufferManager> buffer_manager;
    std::shared_ptr<QueryOperator> source;
    std::shared_ptr<BPTree> bptree;
};

class Join : public QueryOperator
{
public:
    Join(std::shared_ptr<BufferManager> const& buffer_manager, std::shared_ptr<QueryOperator> const& source1,
         std::shared_ptr<QueryOperator> const& source2, int attribute_position1, int attribute_position2,
         std::vector<std::string> const& attribute_types1, std::vector<std::string> const& attribute_types2,
         std::string const& comparator);

    virtual bool open() override;

    virtual std::shared_ptr<Record> next() override;

    virtual bool close() override;

private:
    std::shared_ptr<BufferManager> buffer_manager;
    std::shared_ptr<QueryOperator> source1;
    std::shared_ptr<QueryOperator> source2;
    int attribute_position1;
    int attribute_position2;
    std::vector<std::string> attribute_types1;
    std::vector<std::string> attribute_types2;
    std::string comparator;
};

#endif
