#ifndef TASK_3_RECORD_H
#define TASK_3_RECORD_H

#include <memory>
#include <variant>
#include <vector>
#include <string>

class Record
{
public:
    typedef std::variant<int, std::string, bool> Attribute;

    Record(std::shared_ptr<void> const& data);

    Record(std::string const& record_id, std::vector<Attribute> const& attributes);

    std::string get_record_id();

    std::string get_string_attribute(int attribute_index);

    int get_integer_attribute(int attribute_index);

    bool get_boolean_attribute(int attribute_index);

    std::shared_ptr<void> get_data();

    int get_size();

    int get_hash();

    static int const RECORD_ID_SIZE = 10;

private:
    std::shared_ptr<void> data;
};

#endif //TASK_3_RECORD_H
