/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "encryption.hh"

namespace encryption {

const extern sstring default_key_file_path;

class local_file_provider;

class local_file_provider_factory : public key_provider_factory {
public:
    static shared_ptr<key_provider> find(encryption_context&, const sstring& path);
    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;
};

}
