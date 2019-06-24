/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "supervisor.hh"
#include "log.hh"
#include <seastar/core/print.hh>
#include <csignal>
#include <cstdlib>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

extern logger startlog;

const sstring supervisor::scylla_upstart_job_str("scylla-server");
const sstring supervisor::upstart_job_env("UPSTART_JOB");
const sstring supervisor::systemd_ready_msg("READY=1");
const sstring supervisor::systemd_status_msg_prefix("STATUS");

sstring supervisor::get_upstart_job_env() {
    const char* upstart_job = std::getenv(upstart_job_env.c_str());
    return !upstart_job ? "" : upstart_job;
}

bool supervisor::try_notify_upstart(sstring msg, bool ready) {
    static const sstring upstart_job_str(get_upstart_job_env());

    if (upstart_job_str != scylla_upstart_job_str) {
        return false;
    }

    if (ready) {
        std::raise(SIGSTOP);
    }

    return true;
}

void supervisor::try_notify_systemd(sstring msg, bool ready) {
#ifdef HAVE_LIBSYSTEMD
    if (ready) {
        sd_notify(0, format("{}\n{}={}\n", systemd_ready_msg, systemd_status_msg_prefix, msg).c_str());
    } else {
        sd_notify(0, format("{}={}\n", systemd_status_msg_prefix, msg).c_str());
    }
#endif
}

void supervisor::notify(sstring msg, bool ready) {
    startlog.info("{}", msg);

    if (try_notify_upstart(msg, ready) == true) {
        return;
    } else {
        try_notify_systemd(msg, ready);
    }
}
