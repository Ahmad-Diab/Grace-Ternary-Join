#ifndef EXTERNAL_JOIN_UTILS_H
#define EXTERNAL_JOIN_UTILS_H

#include <string>
#include <vector>
#include <fstream>
#include <cassert>
#include <unistd.h>
#include "TemporaryFile.h"
#if defined(__x86_64__)
#include <x86intrin.h>
#endif

namespace external_join {
    class Utils {
    private:
        static void trim(std::string &str);

        // It was borrowed from Altan Birler
        static uint64_t crc32([[maybe_unused]] uint64_t a, [[maybe_unused]] uint64_t b) noexcept {
            #if defined(__x86_64__)
//                return 0;
                return __builtin_ia32_crc32di(a, b);
            #elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32) && defined(__clang__)
                return __builtin_arm_crc32d(a, b);
            #elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
                return __builtin_aarch64_crc32x(a, b);
            #else
                return 0;
            #endif
        }

    public:
        static std::vector<std::string> read_csv_line(const std::string &line);

        static std::string GetResourcesDirectory();

        // Template for reading a single object of type T

        static bool readBlock(int fd, int64_t offset, void* data, size_t data_size) {
            assert(fd != -1);

            // Use pread to read from the specified offset into the data
            ssize_t bytesRead = pread(fd, data, data_size, offset);
            if (bytesRead == -1) {
//                std::cerr << "Error reading file: " << strerror(errno) << std::endl;
                return false;
            }

            return true;
        }
        static bool readBlock(TemporaryFile* tempFile, int64_t offset, void* data, size_t data_size) {
            assert(tempFile->file_descriptor >= 0);
            return readBlock(tempFile->file_descriptor, offset, data, data_size);
        }

        static bool writeBlock(int fd, int64_t offset, void* data, size_t data_size) {
            assert(fd != -1);
            // Use pwrite to write to the specified offset from the data
            ssize_t bytesWritten = pwrite(fd, data, data_size, offset);
            if (bytesWritten == -1) {
//                std::cerr << "Error writing to file: " << strerror(errno) << std::endl;
                return false;
            }
            return true;
        }

        static bool writeBlock(TemporaryFile* tempFile, int64_t offset, void* data, size_t data_size) {
            assert(tempFile->file_descriptor >= 0);
            return writeBlock(tempFile->file_descriptor, offset, data, data_size);
        }

        template<typename T>
        static bool readBlock(int fd, size_t offset, T &data) {
            assert(fd != -1);

            // Use pread to read from the specified offset into the data
            ssize_t bytesRead = pread(fd, &data, sizeof(T), offset);
            if (bytesRead == -1) {
//                std::cerr << "Error reading file: " << strerror(errno) << std::endl;
                return false;
            }

            return true;
        }

        // Template for writing a single object of type T
        template<typename T>
        static bool writeBlock(int fd, size_t offset, const T &data) {
            assert(fd != -1);

            // Use pwrite to write to the specified offset from the data
            ssize_t bytesWritten = pwrite(fd, &data, sizeof(T), offset);
            if (bytesWritten == -1) {
                std::cerr << "Error writing to file: " << strerror(errno) << std::endl;
                return false;
            }
            return true;
        }

       static inline uint64_t seedForLevel(unsigned int level) {
            const uint64_t baseSeed = 0xDEADBEEFDEADBEEFULL;
            // 0x9E3779B97F4A7C15 is the 64-bit golden ratio constant
            const uint64_t multiplier = 0x9E3779B97F4A7C15ULL;
            return baseSeed ^ (multiplier * (static_cast<uint64_t>(level) + 1));
        }

        // It was borrowed from Altan Birler
        static inline uint64_t hashCRC32(uint64_t i32Word, uint64_t zero = 0) {
            zero = seedForLevel(zero);
            static constexpr uint64_t k0 = 0xC83A91E1, k1 = 0x8648DBDB, k3 = 0x2F5870A5; // Metrohash constants
            auto crc = crc32(zero ^ static_cast<uint32_t>(k0 * k3), i32Word);
            return crc * ((k1 << 32) + 1);
        }
    };
}
#endif //EXTERNAL_JOIN_UTILS_H
