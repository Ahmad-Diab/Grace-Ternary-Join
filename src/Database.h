#ifndef EXTERNAL_JOIN_DATABASE_H
#define EXTERNAL_JOIN_DATABASE_H

#include <string>
#include <unordered_map>
#include "Table.h"

namespace external_join {

    class Database {
    private:
        std::string base_directory;
        std::string config_file;
        std::string db_name;
        std::unordered_map<std::string, Table> tables;

        /// Read the metadata information
        void Read();

    public:
        /// Constructor
        Database() = default;
        /// Destructor
        ~Database() noexcept;

        Database(const Database&) = delete;
        Database& operator=(const Database&) = delete;

        /// Create a new database
        void Create(std::string base_dir, const std::string& name);
        /// Close the database
        void Close();

        /// Has a table?
        bool HasTable(std::string_view name) const { return tables.count(std::string{name}); }
        /// Get a table
        Table& GetTable(std::string_view name);


    };
}

#endif //EXTERNAL_JOIN_DATABASE_H
