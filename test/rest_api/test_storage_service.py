# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary

import pytest
import sys
import requests
import threading
import time

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import unique_name, new_test_table, new_test_keyspace

# "keyspace" function: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
def new_keyspace(cql, this_dc):
    name = unique_name()
    cql.execute(f"CREATE KEYSPACE {name} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}")
    return name

def test_storage_service_auto_compaction_keyspace(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    # test empty keyspace
    resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}")
    resp.raise_for_status()

    resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}")
    resp.raise_for_status()

    # test non-empty keyspace
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t:
        resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}")
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}")
        resp.raise_for_status()

        # non-existing keyspace
        resp = rest_api.send("POST", f"storage_service/auto_compaction/XXX")
        assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_auto_compaction_table(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t:
        test_table = t.split('.')[1]
        resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}", { "cf": test_table })
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": test_table })
        resp.raise_for_status()

        # non-existing table
        resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": "XXX" })
        assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_auto_compaction_tables(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
            test_tables = [t0.split('.')[1], t1.split('.')[1]]
            resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            # non-existing table
            resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
            assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_keyspace_offstrategy_compaction(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}")
        resp.raise_for_status()

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_keyspace_offstrategy_compaction_tables(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
            test_tables = [t0.split('.')[1], t1.split('.')[1]]

            resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            # non-existing table
            resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
            assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_keyspace_scrub(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
            with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
                test_tables = [t0.split('.')[1], t1.split('.')[1]]

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}")
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[1]}" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
                resp.raise_for_status()

                # non-existing table
                resp = rest_api.send("POST", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
                assert resp.status_code == requests.codes.not_found

def test_storage_service_keyspace_scrub_mode(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
            with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
                test_tables = [t0.split('.')[1], t1.split('.')[1]]

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "scrub_mode": "VALIDATE" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "scrub_mode": "XXX" })
                assert resp.status_code == requests.codes.bad_request

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "quarantine_mode": "ONLY" })
                resp.raise_for_status()

                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "cf": f"{test_tables[0]}", "quarantine_mode": "YYY" })
                assert resp.status_code == requests.codes.bad_request

def test_storage_service_keyspace_bad_param(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        # Url must include the keyspace param.
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub")
        assert resp.status_code == requests.codes.not_found

        # Url must include the keyspace param.
        # It cannot be given as an optional param
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub", { "keyspace": "{keyspace}" })
        assert resp.status_code == requests.codes.not_found

        # Optional param cannot use the same name as a mandatory (positional, in url) param.
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "keyspace": "{keyspace}" })
        assert resp.status_code == requests.codes.bad_request

        # Unknown parameter (See https://github.com/scylladb/scylla/pull/10090)
        resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}", { "foo": "bar" })
        assert resp.status_code == requests.codes.bad_request

# Unfortunately by default Python threads print their exceptions
# (e.g., assertion failures) but don't propagate them to the join(),
# so the overall test doesn't fail. The following Thread wrapper
# causes join() to rethrow the exception, so the test will fail.
class ThreadWrapper(threading.Thread):
    def run(self):
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exception = e
    def join(self, timeout=None):
        super().join(timeout)
        if hasattr(self, 'exception'):
            raise self.exception
        return self.ret

# Reproduce issue #9061, where if we have a partition key with characters
# that need escaping in JSON, the toppartitions response failed to escape
# them. The underlying bug was a Seastar bug in JSON in the HTTP server:
# https://github.com/scylladb/seastar/issues/460
def test_toppartitions_pk_needs_escaping(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        with new_test_table(cql, keyspace, "p text PRIMARY KEY") as table:
            # Use a newline character as part of the partition key pk. When
            # toppartitions later returns it, it must escape it (as pk_json)
            # or yield an invalid JSON with a literal newline in a string.
            pk = 'hi\nhello'
            pk_json = r'hi\nhello'
            # Unfortunately, the toppartitions API doesn't let us mark the
            # beginning and end of the sampling period. Instead we need to
            # start the toppartitions for a predefined period, and in
            # parallel, make the request. Sad.
            def toppartitions():
                ks, cf = table.split('.')
                resp = rest_api.send('GET', 'storage_service/toppartitions', {'table_filters': f'{ks}:{cf}', 'duration': '1000'})
                assert resp.ok
                # resp.json() will raise an error if not valid JSON
                resp.json()
                assert pk_json in resp.text
            def insert():
                # We need to wait enough time for the toppartitions request
                # to have been sent, but unfortunately we don't know when
                # this happens because the request doesn't return until the
                # "duration" ends. So we hope 0.5 seconds is enough.
                # TODO: we can use the log to check when the toppartitions
                # request was received.
                time.sleep(0.5)
                stmt = cql.prepare(f"INSERT INTO {table} (p) VALUES (?)")
                cql.execute(stmt, [pk])
            t1 = ThreadWrapper(target=toppartitions)
            t2 = ThreadWrapper(target=insert)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
