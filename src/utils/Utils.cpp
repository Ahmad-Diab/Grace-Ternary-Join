#include "utils/Utils.h"
#include <sstream>
#include "iostream"

using namespace std;

namespace external_join {

    std::vector<std::string> Utils::read_csv_line(const std::string& line) {
        if (!line.empty()) {
            std::vector<std::string> result;
            std::stringstream ss(line);
            std::string field;

            while (std::getline(ss, field, ',')) {
                trim(field);
                result.push_back(field);
            }

            return result;
        }
        return {};
    }

    void Utils::trim(std::string &str) {
        size_t start = str.find_first_not_of(" \t\n\r");
        size_t end = str.find_last_not_of(" \t\n\r");

        if (start == std::string::npos || end == std::string::npos) {
            str =  "";
        }
        else {
            str =  str.substr(start, end - start + 1);
        }
    }
}
