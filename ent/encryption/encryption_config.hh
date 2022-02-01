/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "../../utils/config_file.hh"

namespace encryption {

class encryption_config : public utils::config_file {
public:
    encryption_config();

    typedef std::unordered_map<sstring, string_map> string_string_map;

    named_value<sstring> system_key_directory;
    named_value<bool> config_encryption_active;
    named_value<sstring> config_encryption_key_name;
    named_value<string_map> system_info_encryption;
    named_value<string_string_map> kmip_hosts;
};

}
