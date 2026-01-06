#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace string_utils {

inline std::vector<std::string_view> split(std::string_view str, char delim) {
    std::vector<std::string_view> result;
    size_t start = 0;
    while (start <= str.size()) {
        size_t end = str.find(delim, start);
        if (end == std::string_view::npos) {
            result.push_back(str.substr(start));
            break;
        }
        result.push_back(str.substr(start, end - start));
        start = end + 1;
    }
    return result;
}

inline std::string to_string(std::string_view sv) {
    return std::string(sv);
}

}
