/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "encryption.hh"

namespace encryption {

class replicated_key_provider_factory : public key_provider_factory {
public:
    replicated_key_provider_factory();
    ~replicated_key_provider_factory();

    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;

    static void init();
};

}
