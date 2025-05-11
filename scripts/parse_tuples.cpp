#include <iostream>
#include <fstream>
#include <string>
#include <sstream>

void parseAndWriteBinary(const std::string &inputFile, const std::string &outputFile) {
    std::ifstream infile(inputFile);
    std::ofstream outfile(outputFile, std::ios::binary);

    if (!infile.is_open()) {
        std::cerr << "Error: Could not open input file: " << inputFile << "\n";
        return;
    }

    if (!outfile.is_open()) {
        std::cerr << "Error: Could not open output file: " << outputFile << "\n";
        return;
    }

    std::string line;
    while (std::getline(infile, line)) {
        line.erase(0, line.find_first_not_of(" \t\n\r"));
        line.erase(line.find_last_not_of(" \t\n\r") + 1);

        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string number1, number2;

        if (!std::getline(ss, number1, ',') || !std::getline(ss, number2)) {
            std::cerr << "Skipping invalid line: " << line << "\n";
            continue;
        }

        try {
            uint64_t num1 = std::stoll(number1);
            uint64_t num2 = std::stoll(number2);

            outfile.write(reinterpret_cast<const char*>(&num1), sizeof(uint64_t));
            outfile.write(reinterpret_cast<const char*>(&num2), sizeof(uint64_t));
        } catch (const std::exception &e) {
            std::cerr << "Skipping invalid line due to error: " << e.what() << "\n";
        }
    }

    infile.close();
    outfile.close();
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <output_file>\n";
        return 1;
    }

    std::string inputFile = argv[1];
    std::string outputFile = argv[2];

    parseAndWriteBinary(inputFile, outputFile);

    std::cout << "Parsing and binary writing complete.\n";
    return 0;
}

