#ifndef EXTERNAL_JOIN_TABLE_H
#define EXTERNAL_JOIN_TABLE_H

#include <string>
#include <vector>
#include <memory>
#include <iosfwd>
#include <optional>
#include <cassert>
#include <iosfwd>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <istream>
#include "typedefs.h"
#include "operator/ScanOperator.h"

namespace external_join {

    class Table {
    private:
        std::string name;
        std::string filename;
        std::vector<attribute_name_t> attributes; // name, type = int64_t, size, uniquevalue,
        size_t cardinality;
        int file_descriptor;

    public:
        friend class Database;
        friend class ScanOperator;

        Table();

        Table(std::string name, std::string file);

        Table(const Table &) = delete;

        ~Table() noexcept;

        void AddAttribute(std::string_view name);

        // We assume initially all tables have only two attributes
        void Read(size_t cardinality, const attribute_name_t& first_attribute, const attribute_name_t& second_attribute);

        void InitIO();

        [[nodiscard]] size_t GetCardinality() const { return cardinality; }

        [[nodiscard]] size_t GetAttributeCount() const { return attributes.size(); }

        [[nodiscard]] const attribute_name_t &GetAttribute(unsigned index) const { return attributes[index]; }

        [[nodiscard]] std::optional<size_t> FindAttribute(std::string_view attribute_name) const;

        [[nodiscard]] const std::string& GetName() const { return name; }

    };

}

#endif //EXTERNAL_JOIN_TABLE_H
