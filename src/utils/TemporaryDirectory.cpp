#include "utils/TemporaryDirectory.h"
#include <iostream>
#include <cstdlib>
#include <unistd.h>
#include <cassert>

namespace external_join {

    TemporaryDirectory::TemporaryDirectory() {
        char tmpDirTemplate[] = "/tmp/external-join-dir-XXXXXX";
        char *tmpDirName = mkdtemp(tmpDirTemplate);
        if (!tmpDirName) {
            perror("mkdtemp");
            exit(EXIT_FAILURE);
        }
        name =  std::string(tmpDirName);
        mkdir(name.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        file_descriptor = open(name.c_str(), O_DIRECTORY);
        if (file_descriptor < 0) {
            perror("failed to open temp directory for external join");
        }
    }

    TemporaryDirectory::TemporaryDirectory(const std::string &directory_name,
                                           std::shared_ptr<TemporaryDirectory> parent) {
        assert(parent);
        parent_directory = std::move(parent);
        const char* name_str = directory_name.c_str();
        if (mkdirat(parent_directory->file_descriptor, name_str, S_IRWXU) < 0) {
            perror("failed to create subdirectory");
        }
        name = directory_name;
        file_descriptor = openat(parent_directory->file_descriptor, name_str, O_DIRECTORY);
        if (file_descriptor < 0) {
            perror("failed to open subdirectory");
        }
    }

    TemporaryDirectory::~TemporaryDirectory() {
        if (close(file_descriptor) < 0) {
            perror("failed to close temporary subdirectory");
        }
        if (parent_directory) {
            if (unlinkat(parent_directory->file_descriptor, name.c_str(), AT_REMOVEDIR) < 0) {
                perror("failed to remove subdirectory");
            }
        } else {
            if (rmdir(name.c_str()) < 0) {
                perror("failed to remove directory");
            }
        }
    }

    std::string TemporaryDirectory::getFilename() const {
        return name;
    }
}