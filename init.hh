/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */
#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include "log.hh"
#include "seastarx.hh"
#include <boost/program_options.hpp>

namespace db {
class extensions;
class seed_provider_type;
class config;
namespace view {
class view_update_generator;
}
}

namespace gms {
class feature_service;
class inet_address;
}

extern logging::logger startlog;

class bad_configuration_error : public std::exception {};

std::set<gms::inet_address> get_seeds_from_db_config(const db::config& cfg);

/**
 * Very simplistic config registry. Allows hooking in a config object
 * to the "main" sequence.
 */
class configurable {
public:
    configurable() {
        // We auto register. Not that like cycle is assumed to be forever
        // and scope should be managed elsewhere.
        register_configurable(*this);
    }
    virtual ~configurable()
    {}
    // Hook to add command line options and/or add main config options
    virtual void append_options(db::config&, boost::program_options::options_description_easy_init&)
    {};
    // Called after command line is parsed and db/config populated.
    // Hooked config can for example take this oppurtunity to load any file(s).
    virtual future<> initialize(const boost::program_options::variables_map&) {
        return make_ready_future();
    }
    virtual future<> initialize(const boost::program_options::variables_map& map, const db::config& cfg, db::extensions& exts) {
        return initialize(map);
    }

    /**
     * State of initiating system for optional 
     * notification callback to objects created by
     * `initialize`
     */
    enum class system_state {
        started,
        stopped,
    };

    using notify_func = std::function<future<>(system_state)>;

    /** Hooks should override this to allow adding a notification function to the startup sequence. */
    virtual future<notify_func> initialize_ex(const boost::program_options::variables_map& map, const db::config& cfg, db::extensions& exts) {
        return initialize(map, cfg, exts).then([] { return notify_func{}; });
    }

    class notify_set {
    public:
        future<> notify_all(system_state);
    private:
        friend class configurable;
        std::vector<notify_func> _listeners;
    };

    // visible for testing
    static std::vector<std::reference_wrapper<configurable>>& configurables();
    static future<notify_set> init_all(const boost::program_options::variables_map&, const db::config&, db::extensions&);
    static future<notify_set> init_all(const db::config&, db::extensions&);
    static void append_all(db::config&, boost::program_options::options_description_easy_init&);
private:
    static void register_configurable(configurable &);
};
