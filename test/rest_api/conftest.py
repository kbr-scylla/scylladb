# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an invididual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import pytest
import requests
import ssl
import sys

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import unique_name

# By default, tests run against a Scylla server listening
# on localhost:9042 for CQL and localhost:10000 for the REST API.
# Add the --host, --port, --ssl, or --api-port options to allow overiding these defaults.
def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='localhost',
        help='Scylla server host to connect to')
    parser.addoption('--port', action='store', default='9042',
        help='Scylla CQL port to connect to')
    parser.addoption('--ssl', action='store_true',
        help='Connect to CQL via an encrypted TLSv1.2 connection')
    parser.addoption('--api-port', action='store', default='10000',
        help='server REST API port to connect to')

class RestApiSession:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.session = requests.Session()

    def send(self, method, path, params={}):
        url=f"http://{self.host}:{self.port}/{path}"
        if params:
            sep = '?'
            for key, value in params.items():
                url += f"{sep}{key}={value}"
                sep = '&'
        req = self.session.prepare_request(requests.Request(method, url))
        return self.session.send(req)

# "api" fixture: set up client object for communicating with Scylla API.
# The host/port combination of the server are determined by the --host and
# --port options, and defaults to localhost and 10000, respectively.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def rest_api(request):
    host = request.config.getoption('host')
    port = request.config.getoption('api_port')
    return RestApiSession(host, port)

# "cql" fixture: set up client object for communicating with the CQL API.
# The host/port combination of the server are determined by the --host and
# --port options, and defaults to localhost and 9042, respectively.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def cql(request):
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeout (in seconds) for execute() commands is 10, which
        # should have been more than enough, but in some extreme cases with a
        # very slow debug build running on a very busy machine and a very slow
        # request (e.g., a DROP KEYSPACE needing to drop multiple tables)
        # 10 seconds may not be enough, so let's increase it. See issue #7838.
        request_timeout = 120)
    if request.config.getoption('ssl'):
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[request.config.getoption('host')],
        port=int(request.config.getoption('port')),
        # TODO: make the protocol version an option, to allow testing with
        # different versions. If we drop this setting completely, it will
        # mean pick the latest version supported by the client and the server.
        protocol_version=4,
        # Use the default superuser credentials, which work for both Scylla and Cassandra
        auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
        ssl_context=ssl_context,
    )
    return cluster.connect()

# Until Cassandra 4, NetworkTopologyStrategy did not support the option
# replication_factor (https://issues.apache.org/jira/browse/CASSANDRA-14303).
# We want to allow these tests to run on Cassandra 3.* (for the convenience
# of developers who happen to have it installed), so we'll use the older
# syntax that needs to specify a DC name explicitly. For this, will have
# a "this_dc" fixture to figure out the name of the current DC, so it can be
# used in NetworkTopologyStrategy.
@pytest.fixture(scope="session")
def this_dc(cql):
    yield cql.execute("SELECT data_center FROM system.local").one()[0]

