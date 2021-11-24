/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <algorithm>
#include <seastar/core/sleep.hh>
#include "service_level_controller.hh"
#include "service/priority_manager.hh"
#include "message/messaging_service.hh"
#include "db/system_distributed_keyspace.hh"
#include "utils/fb_utilities.hh"
#include <seastar/core/reactor.hh>

namespace qos {
static logging::logger sl_logger("service_level_controller");

sstring service_level_controller::default_service_level_name = "default";



service_level_controller::service_level_controller(sharded<auth::service>& auth_service, service_level_options default_service_level_config, scheduling_group default_scheduling_group, bool destroy_default_sg_on_drain)
        : _sl_data_accessor(nullptr)
        , _auth_service(auth_service)
        , _last_successful_config_update(seastar::lowres_clock::now())
        , _logged_intervals(0)
{
    if (this_shard_id() == global_controller) {
        _global_controller_db = std::make_unique<global_controller_data>();
        _global_controller_db->default_service_level_config = default_service_level_config;
        _global_controller_db->default_sg = default_scheduling_group;
        _global_controller_db->destroy_default_sg = destroy_default_sg_on_drain;
        // since the first thing that is being done is adding the default service level, we only
        // need to throw the given group to the pool of scheduling groups for reuse.
        _global_controller_db->deleted_scheduling_groups.emplace_back(default_scheduling_group);
    }
}

future<> service_level_controller::add_service_level(sstring name, service_level_options slo, bool is_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, slo, is_static] () {
           return sl_controller.do_add_service_level(name, slo, is_static);
        });
    });
}

future<>  service_level_controller::remove_service_level(sstring name, bool remove_static) {
    return container().invoke_on(global_controller, [=] (service_level_controller &sl_controller) {
        return with_semaphore(sl_controller._global_controller_db->notifications_serializer, 1, [&sl_controller, name, remove_static] () {
           return sl_controller.do_remove_service_level(name, remove_static);
        });
    });
}

future<> service_level_controller::start() {
    if (this_shard_id() != global_controller) {
        return make_ready_future();
    }
    return with_semaphore(_global_controller_db->notifications_serializer, 1, [this] () {
        return do_add_service_level(default_service_level_name, _global_controller_db->default_service_level_config, true).then([this] () {
            return container().invoke_on_all([] (service_level_controller& sl) {
                sl._default_service_level =  sl.get_service_level(default_service_level_name);
                sl.register_with_priority_manager();
            });
        });
    });
}


void service_level_controller::set_distributed_data_accessor(service_level_distributed_data_accessor_ptr sl_data_accessor) {
    // unregistering the accessor is always legal
    if (!sl_data_accessor) {
        _sl_data_accessor = nullptr;
    }

    // Registration of a new accessor can be done only when the _sl_data_accessor is not already set.
    // This behavior is intended to allow to unit testing debug to set this value without having
    // overriden by storage_proxy
    if (!_sl_data_accessor) {
        _sl_data_accessor = sl_data_accessor;
    }
}

future<> service_level_controller::drain() {
    if (this_shard_id() != global_controller) {
        return make_ready_future();
    }
    // abort the loop of the distributed data checking if it is running
    if (!_global_controller_db->dist_data_update_aborter.abort_requested()) {
        _global_controller_db->dist_data_update_aborter.request_abort();
    }
    _global_controller_db->notifications_serializer.broken();
    return std::exchange(_global_controller_db->distributed_data_update, make_ready_future<>()).then([this] {
        // delete all sg's in _service_levels_db, leaving it empty.
        for (auto it = _service_levels_db.begin(); it != _service_levels_db.end(); ) {
            _global_controller_db->deleted_scheduling_groups.emplace_back(it->second.sg);
            _global_controller_db->deleted_priority_classes.emplace_back(it->second.pc);
            it = _service_levels_db.erase(it);
        }
    }).then_wrapped([] (future<> f) {
        //unregister from the priority manager
        service::get_local_priority_manager().set_service_level_controller(nullptr);
        try {
            f.get();
        } catch (const broken_semaphore& ignored) {
        } catch (const sleep_aborted& ignored) {
        } catch (const exceptions::unavailable_exception& ignored) {
        } catch (const exceptions::read_timeout_exception& ignored) {
        }
    });
}

future<> service_level_controller::stop() {
    return drain().finally([this] {
        if (this_shard_id() != global_controller) {
            return make_ready_future<>();
        }

        // exclude scheduling groups we shouldn't destroy
        std::erase_if(_global_controller_db->deleted_scheduling_groups, [this] (scheduling_group& sg) {
            if (sg == default_scheduling_group()) {
                return true;
            } else if (!_global_controller_db->destroy_default_sg && _global_controller_db->default_sg == sg) {
                return true;
            } else {
                return false;
            }
        });
        // destroy all sg's in _global_controller_db->deleted_scheduling_groups, leaving it empty
        // if any destroy_scheduling_group call fails, return one of the exceptions
        return do_with(std::move(_global_controller_db->deleted_scheduling_groups), std::exception_ptr(),
                [] (std::deque<scheduling_group>& deleted_scheduling_groups, std::exception_ptr& ex) {
            return do_until([&] () { return deleted_scheduling_groups.empty(); }, [&] () {
                return destroy_scheduling_group(deleted_scheduling_groups.front()).then_wrapped([&] (future<> f) {
                    if (f.failed()) {
                        auto e = f.get_exception();
                        sl_logger.error("Destroying scheduling group \"{}\" on stop failed: {}.  Ignored.", deleted_scheduling_groups.front().name(), e);
                        ex = std::move(e);
                    }
                    deleted_scheduling_groups.pop_front();
                });
            }).finally([&] () {
                return ex ? make_exception_future<>(std::move(ex)) : make_ready_future<>();
            });
        });
    });
}

void service_level_controller::register_with_priority_manager() {
    service::get_local_priority_manager().set_service_level_controller(this);
}

future<> service_level_controller::update_service_levels_from_distributed_data() {

    if (!_sl_data_accessor) {
        return make_ready_future();
    }

    if (this_shard_id() != global_controller) {
        return make_ready_future();
    }

    return with_semaphore(_global_controller_db->notifications_serializer, 1, [this] () {
        return async([this] () {
            service_levels_info service_levels;
            // The next statement can throw, but that's fine since we would like the caller
            // to be able to agreggate those failures and only report when it is critical or noteworthy.
            // one common reason for failure is because one of the nodes comes down and before this node
            // detects it the scan query done inside this call is failing.
            service_levels = _sl_data_accessor->get_service_levels().get0();

            service_levels_info service_levels_for_add_or_update;
            service_levels_info service_levels_for_delete;

            auto current_it = _service_levels_db.begin();
            auto new_state_it = service_levels.begin();

            // we want to detect 3 kinds of objects in one pass -
            // 1. new service levels that have been added to the distributed keyspace
            // 2. existing service levels that have changed
            // 3. removed service levels
            // this loop is batching together add/update operation and remove operation
            // then they are all executed together.The reason for this is to allow for
            // firstly delete all that there is to be deleted and only then adding new
            // service levels.
            while (current_it != _service_levels_db.end() && new_state_it != service_levels.end()) {
                if (current_it->first == new_state_it->first) {
                    //the service level exists on both the cureent and new state.
                    if (current_it->second.slo != new_state_it->second) {
                        // The service level configuration is different
                        // in the new state and the old state, meaning it needs to be updated.
                        int32_t old_shares = 1000;
                        if (auto old_shares_p = std::get_if<int32_t>(&current_it->second.slo.shares)) {
                            old_shares = *old_shares_p;
                        }
                        int32_t new_shares = 1000;
                        if (auto new_shares_p = std::get_if<int32_t>(&new_state_it->second.shares)) {
                            new_shares = *new_shares_p;
                        }
                        sl_logger.info("service level \"{}\" was updated from {} to {} shares.",
                                new_state_it->first.c_str(), old_shares, new_shares);
                        service_levels_for_add_or_update.insert(*new_state_it);
                    }
                    current_it++;
                    new_state_it++;
                } else if (current_it->first < new_state_it->first) {
                    //The service level does not exists in the new state so it needs to be
                    //removed, but only if it is not static since static configurations dont
                    //come from the distributed keyspace but from code.
                    if (!current_it->second.is_static) {
                        sl_logger.info("service level \"{}\" was deleted.", current_it->first.c_str());
                        service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                    }
                    current_it++;
                } else { /*new_it->first < current_it->first */
                    // The service level exits in the new state but not in the old state
                    // so it needs to be added.
                    sl_logger.info("service level \"{}\" was added.", new_state_it->first.c_str());
                    service_levels_for_add_or_update.insert(*new_state_it);
                    new_state_it++;
                }
            }

            for (; current_it != _service_levels_db.end(); current_it++) {
                if (!current_it->second.is_static) {
                    sl_logger.info("service level \"{}\" was deleted.", current_it->first.c_str());
                    service_levels_for_delete.emplace(current_it->first, current_it->second.slo);
                }
            }
            for (; new_state_it != service_levels.end(); new_state_it++) {
                sl_logger.info("service level \"{}\" was added.", new_state_it->first.c_str());
                service_levels_for_add_or_update.emplace(new_state_it->first, new_state_it->second);
            }

            for (auto&& sl : service_levels_for_delete) {
                do_remove_service_level(sl.first, false).get();
            }
            for (auto&& sl : service_levels_for_add_or_update) {
                do_add_service_level(sl.first, sl.second).get();
            }
        });
    });
}

future<std::optional<service_level_options>> service_level_controller::find_service_level(auth::role_set roles) {
    auto& role_manager = _auth_service.local().underlying_role_manager();

    // converts a list of roles into the chosen service level.
    return ::map_reduce(roles.begin(), roles.end(), [&role_manager, this] (const sstring& role) {
        return role_manager.get_attribute(role, "service_level").then_wrapped([this, role] (future<std::optional<sstring>> sl_name_fut) -> std::optional<service_level_options> {
            try {
                std::optional<sstring> sl_name = sl_name_fut.get0();
                if (!sl_name) {
                    return std::nullopt;
                }
                auto sl_it = _service_levels_db.find(*sl_name);
                if ( sl_it == _service_levels_db.end()) {
                    return std::nullopt;
                }
                auto slo = sl_it->second.slo;
                slo.shares_name = sl_name;
                return slo;
            } catch (...) { // when we fail, we act as if the attribute does not exist so the node
                           // will not be brought down.
                return std::nullopt;
            }
        });
    }, std::optional<service_level_options>{}, [this] (std::optional<service_level_options> first, std::optional<service_level_options> second) -> std::optional<service_level_options> {
        if (!second) {
            return first;
        } else if (!first) {
            return second;
        } else {
            return first->merge_with(*second);
        }
    });
}

future<>  service_level_controller::notify_service_level_added(sstring name, service_level sl_data) {
    return seastar::async( [this, name, sl_data] {
        _subscribers.for_each([name, sl_data] (qos_configuration_change_subscriber* subscriber) {
            try {
                subscriber->on_before_service_level_add(sl_data.slo, {name}).get();
            } catch (...) {
                sl_logger.error("notify_service_level_added: exception occurred in one of the observers callbacks {}", std::current_exception());
            }
        });
        auto result= _service_levels_db.emplace(name, sl_data);
        if (result.second) {
            unsigned sl_idx = internal::scheduling_group_index(sl_data.sg);
            _sl_lookup[sl_idx].first = &(result.first->first);
            _sl_lookup[sl_idx].second = &(result.first->second);
        }
    });

}

future<> service_level_controller::notify_service_level_updated(sstring name, service_level_options slo) {
    auto sl_it = _service_levels_db.find(name);
    future<> f = make_ready_future();
    if (sl_it != _service_levels_db.end()) {
        service_level_options slo_before = sl_it->second.slo;
        return seastar::async( [this,sl_it, name, slo_before, slo] {
            future<> f = make_ready_future();
            _subscribers.for_each([name, slo_before, slo] (qos_configuration_change_subscriber* subscriber) {
                try {
                    subscriber->on_before_service_level_change(slo_before, slo, {name}).get();
                } catch (...) {
                    sl_logger.error("notify_service_level_updated: exception occurred in one of the observers callbacks {}", std::current_exception());
                }
            });
            if (sl_it->second.slo.shares != slo.shares) {
                int32_t new_shares = 1000;
                if (auto new_shares_p = std::get_if<int32_t>(&sl_it->second.slo.shares)) {
                    new_shares = *new_shares_p;
                }
                sl_it->second.sg.set_shares(new_shares);
                f = f.then([pc = sl_it->second.pc, shares = new_shares] () {
                    return engine().update_shares_for_class(pc, shares);
                });
            }

            sl_it->second.slo = slo;
        });
    }
    return f;
}

future<> service_level_controller::notify_service_level_removed(sstring name) {
    auto sl_it = _service_levels_db.find(name);
    if (sl_it != _service_levels_db.end()) {
        unsigned sl_idx = internal::scheduling_group_index(sl_it->second.sg);
        _sl_lookup[sl_idx].first = nullptr;
        _sl_lookup[sl_idx].second = nullptr;
        if (this_shard_id() == global_controller) {
            _global_controller_db->deleted_scheduling_groups.emplace_back(sl_it->second.sg);
            _global_controller_db->deleted_priority_classes.emplace_back(sl_it->second.pc);
        }
        _service_levels_db.erase(sl_it);
        return seastar::async( [this, name] {
            _subscribers.for_each([name] (qos_configuration_change_subscriber* subscriber) {
                try {
                    subscriber->on_after_service_level_remove({name}).get();
                } catch (...) {
                    sl_logger.error("notify_service_level_removed: exception occurred in one of the observers callbacks {}", std::current_exception());
                }
            });
        });
    }
    return make_ready_future<>();
}

io_priority_class* service_level_controller::get_current_priority_class() {
    unsigned sched_idx = internal::scheduling_group_index(current_scheduling_group());
    if (_sl_lookup[sched_idx].second) {
        return &(_sl_lookup[sched_idx].second->pc);
    } else {
        return nullptr;
    }
}

scheduling_group service_level_controller::get_default_scheduling_group() {
    return _default_service_level.sg;
}

scheduling_group service_level_controller::get_scheduling_group(sstring service_level_name) {
    auto service_level_it = _service_levels_db.find(service_level_name);
    if (service_level_it != _service_levels_db.end()) {
        return service_level_it->second.sg;
    } else {
        return get_default_scheduling_group();
    }
}

bool service_level_controller::is_default_service_level() {
    return (current_scheduling_group() == get_default_scheduling_group());
}

std::optional<sstring> service_level_controller::get_active_service_level() {
    unsigned sched_idx = internal::scheduling_group_index(current_scheduling_group());
    if (_sl_lookup[sched_idx].first) {
        return sstring(*_sl_lookup[sched_idx].first);
    } else {
        return std::nullopt;
    }
}

void service_level_controller::update_from_distributed_data(std::chrono::duration<float> interval) {
    if (this_shard_id() != global_controller) {
        throw std::runtime_error(format("Service level updates from distributed data can only be activated on shard {}", global_controller));
    }
    if (_global_controller_db->distributed_data_update.available()) {
        sl_logger.info("update_from_distributed_data: starting configuration polling loop");
        _logged_intervals = 0;
        _global_controller_db->distributed_data_update = repeat([this, interval] {
            return sleep_abortable<steady_clock_type>(std::chrono::duration_cast<steady_clock_type::duration>(interval),
                    _global_controller_db->dist_data_update_aborter).then_wrapped([this] (future<>&& f) {
                try {
                    f.get();
                    return update_service_levels_from_distributed_data().then_wrapped([this] (future<>&& f){
                        try {
                            f.get();
                            _last_successful_config_update = seastar::lowres_clock::now();
                            _logged_intervals = 0;
                        } catch (...) {
                            using namespace std::literals::chrono_literals;
                            constexpr auto age_resolution = 90s;
                            constexpr unsigned error_threshold = 10; // Change the logging level to error after 10 age_resolution intervals.
                            unsigned configuration_age = (seastar::lowres_clock::now() - _last_successful_config_update) / age_resolution;
                            if (configuration_age > _logged_intervals) {
                                log_level ll = configuration_age >= error_threshold ? log_level::error : log_level::warn;
                                sl_logger.log(ll, "update_from_distributed_data: failed to update configuration for more than  {} seconds : {}",
                                        (age_resolution*configuration_age).count(), std::current_exception());
                                _logged_intervals++;
                            }
                        }
                        return stop_iteration::no;
                    });
                } catch (const sleep_aborted& e) {
                    sl_logger.info("update_from_distributed_data: configuration polling loop aborted");
                    return make_ready_future<seastar::bool_class<seastar::stop_iteration_tag>>(stop_iteration::yes);
                }
            });
        }).then_wrapped([] (future<>&& f) {
            try {
                f.get();
            } catch (...) {
                sl_logger.error("update_from_distributed_data: polling loop stopped unexpectedly by: {}",
                        std::current_exception());
            }
        });
    }
}

future<> service_level_controller::add_distributed_service_level(sstring name, service_level_options slo, bool if_not_exists) {
    set_service_level_op_type add_type = if_not_exists ? set_service_level_op_type::add_if_not_exists : set_service_level_op_type::add;
    return set_distributed_service_level(name, slo, add_type);
}

future<> service_level_controller::alter_distributed_service_level(sstring name, service_level_options slo) {
    return set_distributed_service_level(name, slo, set_service_level_op_type::alter);
}

future<> service_level_controller::drop_distributed_service_level(sstring name, bool if_exists) {
    return _sl_data_accessor->get_service_levels().then([this, name, if_exists] (qos::service_levels_info sl_info) {
        auto it = sl_info.find(name);
        if (it == sl_info.end()) {
            if (if_exists) {
                return make_ready_future();
            } else {
                return make_exception_future(nonexistant_service_level_exception(name));
            }
        } else {
            auto& role_manager = _auth_service.local().underlying_role_manager();
            return role_manager.query_attribute_for_all("service_level").then( [&role_manager, name] (auth::role_manager::attribute_vals attributes) {
                return parallel_for_each(attributes.begin(), attributes.end(), [&role_manager, name] (auto&& attr) {
                    if (attr.second == name) {
                        return role_manager.remove_attribute(attr.first, "service_level");
                    } else {
                        return make_ready_future();
                    }
                });
            }).then([this, name] {
                return _sl_data_accessor->drop_service_level(name);
            });
        }
    });
}

future<service_levels_info> service_level_controller::get_distributed_service_levels() {
    return _sl_data_accessor ? _sl_data_accessor->get_service_levels() : make_ready_future<service_levels_info>();
}

future<service_levels_info> service_level_controller::get_distributed_service_level(sstring service_level_name) {
    return _sl_data_accessor ? _sl_data_accessor->get_service_level(service_level_name) : make_ready_future<service_levels_info>();
}

future<> service_level_controller::set_distributed_service_level(sstring name, service_level_options slo, set_service_level_op_type op_type) {
    return _sl_data_accessor->get_service_levels().then([this, name, slo, op_type] (qos::service_levels_info sl_info) {
        auto it = sl_info.find(name);
        // test for illegal requests or requests that should terminate without any action
        if (it == sl_info.end()) {
            if (op_type == set_service_level_op_type::alter) {
                return make_exception_future(exceptions::invalid_request_exception(format("The service level '{}' desn't exist.", name)));
            }
        } else {
            if (op_type == set_service_level_op_type::add) {
                return make_exception_future(exceptions::invalid_request_exception(format("The service level '{}' already exists.", name)));
            } else if (op_type == set_service_level_op_type::add_if_not_exists) {
                return make_ready_future();
            }
        }
        return _sl_data_accessor->set_service_level(name, slo);
    });
}

future<> service_level_controller::do_add_service_level(sstring name, service_level_options slo, bool is_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (is_static) {
        _global_controller_db->static_configurations[name] = slo;
    }
    if (service_level_it != _service_levels_db.end()) {
        if ((is_static && service_level_it->second.is_static) || !is_static) {
           if ((service_level_it->second.is_static) && (!is_static)) {
               service_level_it->second.is_static = false;
           }
           return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, slo);
        } else {
            // this means we set static layer when the the service level
            // is running of the non static configuration. so we have nothing
            // else to do since we already saved the static configuration.
            return make_ready_future();
        }
    } else {
        return do_with(service_level{slo /*slo*/, default_scheduling_group() /*sg*/, default_priority_class() /*pc*/,
                false /*marked_for_deletion*/, is_static /*is_static*/}, std::move(name), [this] (service_level& sl, sstring& name) {
            return make_ready_future().then([this, &sl] () mutable {
                if (!_global_controller_db->deleted_scheduling_groups.empty()) {
                    scheduling_group sg;
                    sg =  _global_controller_db->deleted_scheduling_groups.front();
                    _global_controller_db->deleted_scheduling_groups.pop_front();
                    sl.sg = sg;
                    return container().invoke_on_all([sg, &sl] (service_level_controller& service) {
                        scheduling_group non_const_sg = sg;
                        int32_t shares = 1000;
                        if (auto shares_p = std::get_if<int32_t>(&sl.slo.shares)) {
                            shares = *shares_p;
                        }
                        return non_const_sg.set_shares((float)shares);
                    });
                } else {
                   int32_t shares = 1000;
                   if (auto shares_p = std::get_if<int32_t>(&sl.slo.shares)) {
                       shares = *shares_p;
                   }
                   return create_scheduling_group(format("service_level_sg_{}", _global_controller_db->schedg_group_cnt++), shares).then([&sl] (scheduling_group sg) {
                       sl.sg = sg;
                   });
                }
            }).then([this, &sl] () mutable {
                if (!_global_controller_db->deleted_priority_classes.empty()) {
                    int32_t shares = 1000;
                    if (auto shares_p = std::get_if<int32_t>(&sl.slo.shares)) {
                        shares = *shares_p;
                    }
                    io_priority_class pc = _global_controller_db->deleted_priority_classes.front();
                    _global_controller_db->deleted_priority_classes.pop_front();
                    sl.pc = pc;
                    return container().invoke_on_all([pc, shares] (service_level_controller& service) {
                        return engine().update_shares_for_class(pc, shares);
                    });
                } else {
                    int32_t shares = 1000;
                    if (auto shares_p = std::get_if<int32_t>(&sl.slo.shares)) {
                        shares = *shares_p;
                    }
                    sl. pc = engine().
                            register_one_priority_class(format("service_level_pc_{}", _global_controller_db->io_priority_cnt++), shares);
                    return make_ready_future();
                }
            }).then([this, &sl, name] () {
                return container().invoke_on_all(&service_level_controller::notify_service_level_added, name, sl);
            });
        });
    }
    return make_ready_future();
}

future<> service_level_controller::do_remove_service_level(sstring name, bool remove_static) {
    auto service_level_it = _service_levels_db.find(name);
    if (service_level_it != _service_levels_db.end()) {
        auto static_conf_it = _global_controller_db->static_configurations.end();
        bool static_exists = false;
        if (remove_static) {
            _global_controller_db->static_configurations.erase(name);
        } else {
            static_conf_it = _global_controller_db->static_configurations.find(name);
            static_exists = static_conf_it != _global_controller_db->static_configurations.end();
        }
        if (remove_static && service_level_it->second.is_static) {
            return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
        } else if (!remove_static && !service_level_it->second.is_static) {
            if (static_exists) {
                service_level_it->second.is_static = true;
                return container().invoke_on_all(&service_level_controller::notify_service_level_updated, name, static_conf_it->second);
            } else {
                return container().invoke_on_all(&service_level_controller::notify_service_level_removed, name);
            }
        }
    }
    return make_ready_future();
}

void service_level_controller::on_join_cluster(const gms::inet_address& endpoint) { }

void service_level_controller::on_leave_cluster(const gms::inet_address& endpoint) {
    if (this_shard_id() == global_controller && endpoint == utils::fb_utilities::get_broadcast_address()) {
        _global_controller_db->dist_data_update_aborter.request_abort();
    }
}

void service_level_controller::on_up(const gms::inet_address& endpoint) { }

void service_level_controller::on_down(const gms::inet_address& endpoint) { }

void service_level_controller::register_subscriber(qos_configuration_change_subscriber* subscriber) {
    _subscribers.add(subscriber);
}

future<> service_level_controller::unregister_subscriber(qos_configuration_change_subscriber* subscriber) {
    return _subscribers.remove(subscriber);
}

}
