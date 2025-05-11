#ifndef EXTERNAL_JOIN_TEMPORARYDIRECTORY_H
#define EXTERNAL_JOIN_TEMPORARYDIRECTORY_H

#include <string>
#include <memory>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>

namespace external_join {
    class TemporaryDirectory {
        std::string name;
        std::shared_ptr<TemporaryDirectory> parent_directory;
        int file_descriptor;
    public:
        friend class TemporaryFile;
        friend class Utils;
        // root directory (main external join) randomly generated
        explicit TemporaryDirectory();

        explicit TemporaryDirectory(const std::string& directory_name, std::shared_ptr<TemporaryDirectory> parent);

        TemporaryDirectory(const TemporaryDirectory& other) = delete;
        TemporaryDirectory(TemporaryDirectory&& other) noexcept = delete;

        ~TemporaryDirectory();

        TemporaryDirectory& operator=(const TemporaryDirectory& other) = delete;
        TemporaryDirectory& operator=(TemporaryDirectory&& other) noexcept = delete;

        [[nodiscard]] std::string getFilename() const;
    };
}
#endif //EXTERNAL_JOIN_TEMPORARYDIRECTORY_H
