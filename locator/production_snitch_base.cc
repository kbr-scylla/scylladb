/*
 * Modified by ScyllaDB
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */
#include "locator/production_snitch_base.hh"
#include "db/system_keyspace.hh"
#include "gms/gossiper.hh"
#include "message/messaging_service.hh"
#include "utils/fb_utilities.hh"
#include "db/config.hh"

#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <seastar/core/file.hh>

namespace locator {

production_snitch_base::production_snitch_base(snitch_config cfg)
        : allowed_property_keys({ dc_property_key,
                          rack_property_key,
                          prefer_local_property_key,
                          dc_suffix_property_key }) {
    if (!cfg.properties_file_name.empty()) {
        _prop_file_name = cfg.properties_file_name;
    } else {
        _prop_file_name = db::config::get_conf_sub(snitch_properties_filename).string();
    }
}


sstring production_snitch_base::get_rack(inet_address endpoint) {
    if (endpoint == utils::fb_utilities::get_broadcast_address()) {
        return _my_rack;
    }

    return get_endpoint_info(endpoint,
                             gms::application_state::RACK,
                             default_rack);
}

sstring production_snitch_base::get_datacenter(inet_address endpoint) {
    if (endpoint == utils::fb_utilities::get_broadcast_address()) {
        return _my_dc;
    }

    return get_endpoint_info(endpoint,
                             gms::application_state::DC,
                             default_dc);
}

void production_snitch_base::set_backreference(snitch_ptr& d) {
    _backreference = &d;
}

std::optional<sstring> production_snitch_base::get_endpoint_info(inet_address endpoint, gms::application_state key) {
    gms::gossiper& local_gossiper = local().get_local_gossiper();
    auto* ep_state = local_gossiper.get_application_state_ptr(endpoint, key);
    return ep_state ? std::optional(ep_state->value) : std::nullopt;
}

sstring production_snitch_base::get_endpoint_info(inet_address endpoint, gms::application_state key,
                                                  const sstring& default_val) {
    auto val = get_endpoint_info(endpoint, key);
    auto& gossiper = local().get_local_gossiper();

    if (val) {
        return *val;
    }
    // ...if not found - look in the SystemTable...
    if (!_saved_endpoints) {
        _saved_endpoints = gossiper.get_system_keyspace().local().load_dc_rack_info();
    }

    auto it = _saved_endpoints->find(endpoint);

    if (it != _saved_endpoints->end()) {
        if (key == gms::application_state::RACK) {
            return it->second.rack;
        } else { // gms::application_state::DC
            return it->second.dc;
        }
    }

    // ...if still not found - return a default value
    return default_val;
}

void production_snitch_base::set_my_dc_and_rack(const sstring& new_dc, const sstring& new_rack) {
    if (!new_dc.empty()) {
        _my_dc = new_dc;
    } else {
        _my_dc = default_dc;
        logger().warn("{} snitch attempted to set DC to an empty string, falling back to {}.", get_name(), default_dc);
    }

    if (!new_rack.empty()) {
        _my_rack = new_rack;
    } else {
        _my_rack = default_rack;
        logger().warn("{} snitch attempted to set rack to an empty string, falling back to {}.", get_name(), default_rack);
    }
}

void production_snitch_base::set_prefer_local(bool prefer_local) {
    _prefer_local = prefer_local;
}

future<> production_snitch_base::load_property_file() {
    return open_file_dma(_prop_file_name, open_flags::ro)
    .then([this] (file f) {
        return do_with(std::move(f), [this] (file& f) {
            return f.size().then([this, &f] (size_t s) {
                _prop_file_size = s;

                return f.dma_read_exactly<char>(0, s);
            });
        }).then([this] (temporary_buffer<char> tb) {
            _prop_file_contents = std::string(tb.get(), _prop_file_size);
            parse_property_file();

            return make_ready_future<>();
        });
    });
}

void production_snitch_base::parse_property_file() {
    using namespace boost::algorithm;

    std::string line;
    std::istringstream istrm(_prop_file_contents);
    std::vector<std::string> split_line;
    _prop_values.clear();

    while (std::getline(istrm, line)) {
        trim(line);

        // Skip comments or empty lines
        if (!line.size() || line.at(0) == '#') {
            continue;
        }

        split_line.clear();
        split(split_line, line, is_any_of("="));

        if (split_line.size() != 2) {
            throw_bad_format(line);
        }

        auto key = split_line[0]; trim(key);
        auto val = split_line[1]; trim(val);

        if (val.empty() || !allowed_property_keys.contains(key)) {
            throw_bad_format(line);
        }

        if (_prop_values.contains(key)) {
            throw_double_declaration(key);
        }

        _prop_values[key] = val;
    }
}

[[noreturn]]
void production_snitch_base::throw_double_declaration(const sstring& key) const {
    logger().error("double \"{}\" declaration in {}", key, _prop_file_name);
    throw bad_property_file_error();
}

[[noreturn]]
void production_snitch_base::throw_bad_format(const sstring& line) const {
    logger().error("Bad format in properties file {}: {}", _prop_file_name, line);
    throw bad_property_file_error();
}

[[noreturn]]
void production_snitch_base::throw_incomplete_file() const {
    logger().error("Property file {} is incomplete. Some obligatory fields are missing.", _prop_file_name);
    throw bad_property_file_error();
}

} // namespace locator
