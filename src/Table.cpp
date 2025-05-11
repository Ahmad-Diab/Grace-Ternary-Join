#include "Table.h"
#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
using namespace std;

namespace external_join {

    Table::Table(): cardinality(0) {}

    Table::Table(string name, string file)
            : name(std::move(name)), filename(std::move(file)), cardinality(0) {}

    Table::~Table() noexcept = default;

    void Table::AddAttribute(std::string_view attribute_name) {
        attributes.emplace_back(attribute_name);
    }

    void Table::InitIO() {
        file_descriptor = open(filename.c_str(), O_RDONLY);
        if (file_descriptor < 0) {
            throw std::runtime_error("Failed to open file: " + filename);
        }
    }

    void Table::Read(const size_t new_cardinality, const attribute_name_t& first_attribute,
                     const attribute_name_t& second_attribute) {
//         Assume they will be created inside Database::read
//        name = table_name;
//        filename = base_dir + name + ".csv";

        cardinality = new_cardinality;
        attributes.clear();
        AddAttribute(first_attribute);
        AddAttribute(second_attribute);
        InitIO();
    }

    std::optional<size_t> Table::FindAttribute(std::string_view attribute_name) const {
        for (size_t index = 0; index < attributes.size(); ++index)
            if (attributes[index] == attribute_name)
                return index;
        return {};
    }


} // namespace external_join