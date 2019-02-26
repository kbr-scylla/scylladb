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

#include "../../utils/config_file.hh"

namespace encryption {

class encryption_config : public utils::config_file {
public:
    encryption_config();

    typedef std::unordered_map<sstring, string_map> string_string_map;

    named_value<sstring> system_key_directory;
    named_value<bool, value_status::Used> config_encryption_active;
    named_value<sstring, value_status::Used> config_encryption_key_name;
    named_value<string_map> system_info_encryption;
    named_value<string_string_map, value_status::Used> kmip_hosts;
};

}
