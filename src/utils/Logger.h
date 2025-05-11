//
// Created by Ahmed Diab on 13.02.25.
//

#ifndef EXTERNAL_JOIN_LOGGER_H
#define EXTERNAL_JOIN_LOGGER_H

#include <string>
#include <format>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>

namespace external_join {

    class Logger {
    public:
        template<typename... Args>
        static void debug(std::format_string<Args...> message, Args&&... args) {
            #ifdef NDEBUG
                // nondebug
            #elif LOGGER_ON
                log("DEBUG", COLOR_DEBUG, message, std::forward<Args>(args)...);
            #endif
        }

        template<typename... Args>
        static void info(std::format_string<Args...>  message, Args&&... args) {
            #ifdef LOGGER_ON
                log("INFO", COLOR_INFO, message, std::forward<Args>(args)...);
            #endif
        }

        template<typename... Args>
        static void warning(std::format_string<Args...>  message, Args&&... args) {
            #ifdef LOGGER_ON
                log("WARNING", COLOR_WARNING, message, std::forward<Args>(args)...);
            #endif
        }

        template<typename... Args>
        static void error(std::format_string<Args...>  message, Args&&... args) {
            #ifdef LOGGER_ON
                log("ERROR", COLOR_ERROR, message, std::forward<Args>(args)...);
            #endif
        }

    private:
        // ANSI color codes
        static constexpr std::string COLOR_RESET =  "\033[0m";
        static constexpr std::string COLOR_INFO =  "\033[32m";
        static constexpr std::string COLOR_WARNING =  "\033[33m";
        static constexpr std::string COLOR_ERROR =  "\033[31m";
        static constexpr std::string COLOR_DEBUG =  "\033[34m";
        static inline std::mutex loggerLock;

        template<typename... Args>
        static void log(const std::string& level, const std::string& color, std::format_string<Args...> message, Args&&... args) {
            std::string formatted_message = std::format(message, std::forward<Args>(args)...);

            auto now = std::chrono::system_clock::now();
            std::time_t now_time = std::chrono::system_clock::to_time_t(now);

            std::lock_guard<std::mutex> l(loggerLock);
            std::cout << color << "[" << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S")
                      << "] [" << level << "] " << formatted_message << COLOR_RESET << std::endl;
        }
    };
}
#endif //EXTERNAL_JOIN_LOGGER_H
