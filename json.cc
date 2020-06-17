/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "json.hh"

namespace seastar {
namespace json {

static inline bool is_control_char(char c) {
    return c >= 0 && c <= 0x1F;
}

static inline bool needs_escaping(const sstring& s) {
    return std::any_of(s.begin(), s.end(), [](char c) {return is_control_char(c) || c == '"' || c == '\\';});
}

sstring value_to_quoted_string(const sstring& value) {
    if (!needs_escaping(value)) {
        return format("\"{}\"", value);
    }
    std::ostringstream oss;
    oss << std::hex << std::uppercase << std::setfill('0');
    oss.put('"');
    for (char c : value) {
        switch (c) {
        case '"':
            oss.put('\\').put('"');
            break;
        case '\\':
            oss.put('\\').put('\\');
            break;
        case '\b':
            oss.put('\\').put('b');
            break;
        case '\f':
            oss.put('\\').put('f');
            break;
        case '\n':
            oss.put('\\').put('n');
            break;
        case '\r':
            oss.put('\\').put('r');
            break;
        case '\t':
            oss.put('\\').put('t');
            break;
        default:
            if (is_control_char(c)) {
                oss.put('\\').put('u') << std::setw(4) << static_cast<int>(c);
            } else {
                oss.put(c);
            }
            break;
        }
    }
    oss.put('"');
    return oss.str();
}

}

}
