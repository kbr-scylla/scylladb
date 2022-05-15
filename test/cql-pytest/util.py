# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
##################################################################

# Various utility functions which are useful for multiple tests.
# Note that fixtures aren't here - they are in conftest.py.

import string
import random
import time
import socket
import os
from contextlib import contextmanager

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

def random_bytes(length=10):
    return bytearray(random.getrandbits(8) for _ in range(length))

# A function for picking a unique name for test keyspace or table.
# This name doesn't need to be quoted in CQL - it only contains
# lowercase letters, numbers, and underscores, and starts with a letter.
unique_name_prefix = 'cql_test_'
def unique_name():
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms)
unique_name.last_ms = 0

# Functions for picking a unique key to use when multiple tests want to use
# the same shared table and need to pick different keys so as not to collide.
# Because different runs do not share the same table (unique_name() above
# is used to pick the table name), the uniqueness of the keys we generate
# here does not need to be global - we can just use a simple counter to
# guarantee uniqueness.
def unique_key_string():
    unique_key_string.i += 1
    return 's' + str(unique_key_string.i)
unique_key_string.i = 0

def unique_key_int():
    unique_key_int.i += 1
    return unique_key_int.i
unique_key_int.i = 0

# A utility function for creating a new temporary keyspace with given options.
# It can be used in a "with", as:
#   with new_test_keyspace(cql, '...') as keyspace:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_keyspace(cql, opts):
    keyspace = unique_name()
    cql.execute("CREATE KEYSPACE " + keyspace + " " + opts)
    try:
        yield keyspace
    finally:
        cql.execute("DROP KEYSPACE " + keyspace)

# A utility function for creating a new temporary table with a given schema.
# It can be used in a "with", as:
#   with new_test_table(cql, keyspace, '...') as table:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_table(cql, keyspace, schema, extra=""):
    table = keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table + "(" + schema + ")" + extra)
    try:
        yield table
    finally:
        cql.execute("DROP TABLE " + table)


# A utility function for creating a new temporary user-defined function.
@contextmanager
def new_function(cql, keyspace, body, name=None):
    fun = name if name else unique_name()
    cql.execute(f"CREATE FUNCTION {keyspace}.{fun} {body}")
    try:
        yield fun
    finally:
        cql.execute(f"DROP FUNCTION {keyspace}.{fun}")

# A utility function for creating a new temporary user-defined aggregate.
@contextmanager
def new_aggregate(cql, keyspace, body):
    aggr = unique_name()
    cql.execute(f"CREATE AGGREGATE {keyspace}.{aggr} {body}")
    try:
        yield aggr
    finally:
        cql.execute(f"DROP AGGREGATE {keyspace}.{aggr}")

# A utility function for creating a new temporary materialized view in
# an existing table.
@contextmanager
def new_materialized_view(cql, table, select, pk, where):
    keyspace = table.split('.')[0]
    mv = keyspace + "." + unique_name()
    cql.execute(f"CREATE MATERIALIZED VIEW {mv} AS SELECT {select} FROM {table} WHERE {where} PRIMARY KEY ({pk})")
    try:
        yield mv
    finally:
        cql.execute(f"DROP MATERIALIZED VIEW {mv}")

# A utility function for creating a new temporary secondary index of
# an existing table.
@contextmanager
def new_secondary_index(cql, table, column, name='', extra=''):
    keyspace = table.split('.')[0]
    if not name:
        name = unique_name()
    cql.execute(f"CREATE INDEX {name} ON {table} ({column}) {extra}")
    try:
        yield f"{keyspace}.{name}"
    finally:
        cql.execute(f"DROP INDEX {keyspace}.{name}")

def project(column_name_string, rows):
    """Returns a list of column values from each of the rows."""
    return [getattr(r, column_name_string) for r in rows]

# Utility function for trying to find a local process which is listening to
# the address and port to which our our CQL connection is connected. If such a
# process exists, return its process id (as a string). Otherwise, return None.
# Note that the local process needs to belong to the same user running this
# test, or it cannot be found.
def local_process_id(cql):
    ip = socket.gethostbyname(cql.cluster.contact_points[0])
    port = cql.cluster.port
    # Implement something like the shell "lsof -Pni @{ip}:{port}", just
    # using /proc without any external shell command.
    # First, we look in /proc/net/tcp for a LISTEN socket (state 0x0A) at the
    # desired local address. The address is specially-formatted hex of the ip
    # and port, with 0100007F:2352 for 127.0.0.1:9042. We check for two
    # listening addresses: one is the specific IP address given, and the
    # other is listening on address 0 (INADDR_ANY).
    ip2hex = lambda ip: ''.join([f'{int(x):02X}' for x in reversed(ip.split('.'))])
    port2hex = lambda port: f'{int(port):04X}'
    addr1 = ip2hex(ip) + ':' + port2hex(port)
    addr2 = ip2hex('0.0.0.0') + ':' + port2hex(port)
    LISTEN = '0A'
    with open('/proc/net/tcp', 'r') as f:
        for line in f:
            cols = line.split()
            if cols[3] == LISTEN and (cols[1] == addr1 or cols[1] == addr2):
                inode = cols[9]
                break
        else:
            # Didn't find a process listening on the given address
            return None
    # Now look in /proc/*/fd/* for processes that have this socket "inode"
    # as one of its open files. We can only find a process that belongs to
    # the same user.
    target = f'socket:[{inode}]'
    for proc in os.listdir('/proc'):
        if not proc.isnumeric():
            continue
        dir = f'/proc/{proc}/fd/'
        try:
            for fd in os.listdir(dir):
                if os.readlink(dir + fd) == target:
                    # Found the process!
                    return proc
        except:
            # Ignore errors. We can't check processes we don't own.
            pass
    return None
