#include "utils/TemporaryFile.h"

#include <iostream>
#include "boost/uuid.hpp"
#include <cassert>
namespace external_join {

    TemporaryFile::TemporaryFile(std::unique_ptr<TemporaryDirectory>* tempDirectory) :
        parent_directory(tempDirectory) {
        boost::uuids::random_generator gen;
        name = ("external-bucket-file-" + boost::uuids::to_string(gen()));
        this->openFile();
        assert(this->file_descriptor);

    }

    TemporaryFile::~TemporaryFile() {
        closeFile();
        if (unlinkat(parent_directory->get()->file_descriptor, name.c_str(), 0) < 0) {
            perror("removeFile failed");
        }
    }

    void TemporaryFile::openFile() {
        assert(file_descriptor == -1);
        file_descriptor = openat(parent_directory->get()->file_descriptor, name.c_str(), O_RDWR | O_CREAT, 0600);
        if (file_descriptor < 0) {
            perror("openat");
            exit(EXIT_FAILURE);
        }
    }

    void TemporaryFile::closeFile() {
        if (file_descriptor == - 1) {
            return;
        }
        if (close(file_descriptor) < 0) {
            perror("closeFd failed");
        }
        file_descriptor = -1;
    }

    bool TemporaryFile::isFileCreated() {
        return file_descriptor != -1;
    }
}