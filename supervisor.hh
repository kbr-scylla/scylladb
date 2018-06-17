/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "seastarx.hh"

class supervisor {
public:
    static const sstring scylla_upstart_job_str;
    static const sstring upstart_job_env;
    static const sstring systemd_ready_msg;
    /** A systemd status message has a format <status message prefix>=<message> */
    static const sstring systemd_status_msg_prefix;
public:
    /**
     * @brief Notify the Supervisor with the given message.
     * @param msg message to notify the Supervisor with
     * @param ready set to TRUE when scylla service becomes ready
     */
    static void notify(sstring msg, bool ready = false);

private:
    static void try_notify_systemd(sstring msg, bool ready);
    static bool try_notify_upstart(sstring msg, bool ready);
    static sstring get_upstart_job_env();
};
