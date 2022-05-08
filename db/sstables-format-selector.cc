/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "sstables-format-selector.hh"
#include "log.hh"
#include "replica/database.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "gms/versioned_value.hh"
#include "db/system_keyspace.hh"

namespace db {

static logging::logger logger("format_selector");
static const sstring SSTABLE_FORMAT_PARAM_NAME = "sstable_format";

void feature_enabled_listener::on_enabled() {
    if (!_started) {
        _started = true;
        // FIXME -- discarded future
        (void)_selector.maybe_select_format(_format);
    }
}

sstables_format_selector::sstables_format_selector(gms::gossiper& g, sharded<gms::feature_service>& f, sharded<replica::database>& db)
    : _gossiper(g)
    , _features(f)
    , _db(db)
    , _md_feature_listener(*this, sstables::sstable_version_types::md)
    , _me_feature_listener(*this, sstables::sstable_version_types::me)
{ }

future<> sstables_format_selector::maybe_select_format(sstables::sstable_version_types new_format) {
    return with_gate(_sel, [this, new_format] {
        return do_maybe_select_format(new_format);
    });
}

future<> sstables_format_selector::do_maybe_select_format(sstables::sstable_version_types new_format) {
    return with_semaphore(_sem, 1, [this, new_format] {
        if (new_format <= _selected_format) {
            return make_ready_future<bool>(false);
        }
        return db::system_keyspace::set_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME, to_string(new_format)).then([this, new_format] {
            return select_format(new_format);
        }).then([] { return true; });
    }).then([this] (bool update_features) {
        if (!update_features) {
            return make_ready_future<>();
        }
        return _gossiper.add_local_application_state(gms::application_state::SUPPORTED_FEATURES,
                 gms::versioned_value::supported_features(_features.local().supported_feature_set()));
    });
}

future<> sstables_format_selector::start() {
    assert(this_shard_id() == 0);
    return read_sstables_format().then([this] {
        _features.local().me_sstable.when_enabled(_me_feature_listener);
        _features.local().md_sstable.when_enabled(_md_feature_listener);
        return make_ready_future<>();
    });
}

future<> sstables_format_selector::stop() {
    return _sel.close();
}

future<> sstables_format_selector::read_sstables_format() {
    return db::system_keyspace::get_scylla_local_param(SSTABLE_FORMAT_PARAM_NAME).then([this] (std::optional<sstring> format_opt) {
        if (format_opt) {
            sstables::sstable_version_types format = sstables::from_string(*format_opt);
            return select_format(format);
        }
        return make_ready_future<>();
    });
}

future<> sstables_format_selector::select_format(sstables::sstable_version_types format) {
    logger.info("Selected {} sstables format", to_string(format));
    _selected_format = format;
    return _db.invoke_on_all([this] (replica::database& db) {
        db.set_format(_selected_format);
    });
}

} // namespace sstables
