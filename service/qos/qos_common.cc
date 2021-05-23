/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "qos_common.hh"
#include "utils/overloaded_functor.hh"
namespace qos {

service_level_options service_level_options::replace_defaults(const service_level_options& default_values) const {
    service_level_options ret = *this;
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            // reset the value to the default one
            ret.timeout = default_values.timeout;
        },
        [&] (const delete_marker& dm) {
            // remove the value
            ret.timeout = unset_marker{};
        },
        [&] (const lowres_clock::duration&) {
            // leave the value as is
        },
    }, ret.timeout);
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            // reset the value to the default one
            ret.shares = default_values.shares;
        },
        [&] (const delete_marker& dm) {
            // remove the value
            ret.shares = unset_marker{};
        },
        [&] (const int32_t&) {
            // leave the value as is
        },
    }, ret.shares);
    return ret;
}

service_level_options service_level_options::merge_with(const service_level_options& other) const {
    service_level_options ret = *this;
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            ret.timeout = other.timeout;
        },
        [&] (const delete_marker& dm) {
            ret.timeout = other.timeout;
        },
        [&] (const lowres_clock::duration& d) {
            if (auto* other_timeout = std::get_if<lowres_clock::duration>(&other.timeout)) {
                ret.timeout = std::min(d, *other_timeout);
            }
        },
    }, ret.timeout);
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            ret.shares = other.shares;
        },
        [&] (const delete_marker& dm) {
            ret.shares = other.shares;
        },
        [&] (const int32_t& s) {
            if (auto* other_shares = std::get_if<int32_t>(&other.shares)) {
                ret.shares = std::min(s, *other_shares);
                if (ret.shares == other.shares) {
                    ret.shares_name = other.shares_name;
                }
            }
        },
    }, ret.shares);
    return ret;
}

}
