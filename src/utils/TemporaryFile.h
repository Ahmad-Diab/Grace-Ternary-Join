#ifndef EXTERNAL_JOIN_TEMPFILE_H
#define EXTERNAL_JOIN_TEMPFILE_H

#include <string>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <memory>
#include <system_error>
#include "TemporaryDirectory.h"
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cerrno>

namespace external_join {

    class TemporaryFile {
    public:
        std::string name;
        int file_descriptor = -1;
        std::unique_ptr<TemporaryDirectory>* parent_directory;

    public:
        explicit TemporaryFile(std::unique_ptr<TemporaryDirectory>* tempDirectory);

        TemporaryFile(const TemporaryFile& other) = delete;
        TemporaryFile(TemporaryFile&& other) noexcept = default;
        ~TemporaryFile();
        TemporaryFile& operator=(const TemporaryFile& other) = delete;
        TemporaryFile& operator=(TemporaryFile&& other) noexcept = default;

        void openFile();
        void closeFile();
        bool isFileCreated();
    };
}
#endif //EXTERNAL_JOIN_TEMPFILE_H