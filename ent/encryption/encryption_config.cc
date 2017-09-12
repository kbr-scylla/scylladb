/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/filesystem.hpp>

#include "db/config.hh"
#include "utils/config_file_impl.hh"

#include "init.hh"
#include "encryption_config.hh"

encryption::encryption_config::encryption_config()
                : config_file( { system_key_directory, config_encryption_active,
                                config_encryption_key_name,
                                system_info_encryption, kmip_hosts })
// BEGIN entry definitions

                                , system_key_directory("system_key_directory",
                                "resources/system_keys",
                                R"foo(The directory where system keys are kept

Keys used for sstable encryption must be distributed to all nodes
ENT must be able to read and write to the directory.

This directory should have 700 permissions and belong to the scylla user)foo")

                                , config_encryption_active("config_encryption_active", false, "")

                                , config_encryption_key_name("config_encryption_key_name", "system_key",
                                "Set to the local encryption key filename or KMIP key URL to use for configuration file property value decryption")

                                , system_info_encryption(
                                "system_info_encryption",
                                { { "enabled", "false" }, { "cipher_algorithm",
                                                "AES" }, {
                                                "secret_key_strength", "128" },
                                                { "chunk_length_kb", "64" } },
                                R"foo(System information encryption settings

If enabled, system tables that may contain sensitive information (system.batchlog,
system.paxos), hints files and commit logs are encrypted with the
encryption settings below.

When enabling system table encryption on a node with existing data, run
`nodetool upgradesstables -a` on the listed tables to encrypt existing data (NOT IMPLEMENTED)

When tracing is enabled, sensitive info will be written into the tables in the
system_traces keyspace. Those tables should be configured to encrypt their data
on disk.

It is recommended to use remote encryption keys from a KMIP server when using Transparent Data Encryption (TDE) features.
Local key support is provided when a KMIP server is not available.)foo")
                                , kmip_hosts("kmip_hosts", { },
                                R"foo(The unique name of this kmip host/cluster which is specified in the table schema.
host.yourdomain.com={ hosts=[<host1>, <host2>...], keyfile=/path/to/keyfile, truststore=/path/to/truststore.pem, key_cache_millis=<cache ms>, timeout=<timeout ms> }:...

The current implementation of KMIP connection management only supports failover, so all requests will
go through a single KMIP server. There is no load balancing. This is because there aren't any KMIP servers
available (that we've found) that support read replication, or other strategies for availability.

Hosts are tried in the order they appear here. So add them in the same sequence they'll fail over in

Keys read from the KMIP hosts are cached locally for the period of time specified below.
The longer keys are cached, the fewer requests are made to the key server, but the longer
it takes for changes (ie: revocation) to propagate to the node

)foo")
// END entry definitions
{}

static class : public encryption::encryption_config, public configurable {
public:
    void append_options(db::config& cfg, boost::program_options::options_description_easy_init& init) {
        // hook into main scylla.yaml.
        cfg.add(values());
    }
    future<> initialize(const boost::program_options::variables_map& opts, const db::config& cfg, db::extensions& exts) override {
        return make_ready_future();
    }
} cfg;

