#include "Database.h"
#include "utils/Utils.h"

#include <fstream>

using namespace std;

namespace {
    constexpr size_t TABLE_NAME_INDEX = 0;
    constexpr size_t CARDINALITY_INDEX = 1;
    constexpr size_t FIRST_ATTRIBUTE_INDEX = 2;
    constexpr size_t SECOND_ATTRIBUTE_INDEX = 3;

    string GetTableName(const vector<string>& table_info) {
        return table_info[TABLE_NAME_INDEX];
    }

    size_t GetTableCardinality(const vector<string>& table_info) {
        return stoi(table_info[CARDINALITY_INDEX]);
    }

    string GetFirstAttributeName(const vector<string>& table_info) {
        return table_info[FIRST_ATTRIBUTE_INDEX];
    }

    string GetSecondAttributeName(const vector<string>& table_info) {
        return table_info[SECOND_ATTRIBUTE_INDEX];
    }
}
namespace external_join {

    Database::~Database() noexcept {
        Close();
    }

    void Database::Create(std::string base_dir, const std::string& config) {
        Close();

        if (!base_dir.empty() && !config.empty()) {
            base_directory = std::move(base_dir);
            config_file = base_directory + config;
            Read();
        }
    }

    void Database::Read() {
        ifstream in(config_file);
        if (!in.is_open()) {
            throw std::runtime_error("unable to read file :" + config_file);
        }
        getline(in, db_name);
        string table_info_line;
        // Skip line
        getline(in, table_info_line);
        while (getline(in, table_info_line)) {
            vector<string> table_info = Utils::read_csv_line(table_info_line);
            string table_name = GetTableName(table_info);
            Table& table = tables[table_name];
            table.name = table_name;
            table.filename = base_directory + table_name + ".bin";

            table.Read(GetTableCardinality(table_info), GetFirstAttributeName(table_info), GetSecondAttributeName(table_info));
        }
    }

    void Database::Close() {
        config_file = "";
        base_directory = "";
        db_name = "";
        tables.clear();

    }

    Table &Database::GetTable(std::string_view name) {
        if (!tables.count(string{name})) {
            throw std::runtime_error("table not found with name " + string{name});
        }
        return tables[string{name}];

    }
}