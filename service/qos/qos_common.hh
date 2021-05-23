/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <map>
#include <stdexcept>
#include <variant>
#include <seastar/core/lowres_clock.hh>
#include <optional>
#include "exceptions/exceptions.hh"

namespace qos {

/**
 *  a structure that holds the configuration for
 *  a service level.
 */
struct service_level_options {
    struct unset_marker {
        bool operator==(const unset_marker&) const { return true; };
        bool operator!=(const unset_marker&) const { return false; };
    };
    struct delete_marker {
        bool operator==(const delete_marker&) const { return true; };
        bool operator!=(const delete_marker&) const { return false; };
    };

    using timeout_type = std::variant<unset_marker, delete_marker, lowres_clock::duration>;
    timeout_type timeout = unset_marker{};

    using shares_type = std::variant<unset_marker, delete_marker, int32_t>;
    shares_type shares = unset_marker{};

    std::optional<sstring> shares_name; // service level name, if shares is set

    service_level_options replace_defaults(const service_level_options& other) const;
    // Merges the values of two service level options. The semantics depends
    // on the type of the parameter - e.g. for timeouts, a min value is preferred.
    service_level_options merge_with(const service_level_options& other) const;

    bool operator==(const service_level_options& other) const = default;
    bool operator!=(const service_level_options& other) const = default;
};

using service_levels_info = std::map<sstring, service_level_options>;

///
/// A logical argument error for a service_level statement operation.
///
class service_level_argument_exception : public exceptions::invalid_request_exception {
public:
    using exceptions::invalid_request_exception::invalid_request_exception;
};

///
/// An exception to indicate that the service level given as parameter doesn't exist.
///
class nonexistant_service_level_exception : public service_level_argument_exception {
public:
    nonexistant_service_level_exception(sstring service_level_name)
            : service_level_argument_exception(format("Service Level {} doesn't exists.", service_level_name)) {
    }
};

}
