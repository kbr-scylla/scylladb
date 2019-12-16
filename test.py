#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#
import asyncio
import os
import sys
import signal
import argparse
import subprocess
import io
import multiprocessing
import xml.etree.ElementTree as ET
import shutil
import signal
import shlex
import time
from random import randint

LDAP_SERVER_CONFIGURATION_FILE = os.path.join(os.path.dirname(__file__), 'tests', 'slapd.conf')

DEFAULT_ENTRIES = [
    """dn: dc=example,dc=com
objectClass: dcObject
objectClass: organization
dc: example
o: Example
description: Example directory.
""",
    """dn: cn=root,dc=example,dc=com
objectClass: organizationalRole
cn: root
description: Directory manager.
""",
    """dn: ou=People,dc=example,dc=com
objectClass: organizationalUnit
ou: People
description: Our people.
""",
    """# Default superuser for Scylla
dn: uid=cassandra,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: cassandra
ou: People
sn: cassandra
userid: cassandra
userPassword: cassandra
""",
    """dn: uid=jsmith,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: Joe Smith
ou: People
sn: Smith
userid: jsmith
userPassword: joeisgreat
""",
    """dn: uid=jdoe,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: John Doe
ou: People
sn: Doe
userid: jdoe
userPassword: pa55w0rd
""",
    """dn: cn=role1,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role1
uniqueMember: uid=jsmith,ou=People,dc=example,dc=com
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
""",
    """dn: cn=role2,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role2
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
""",
    """dn: cn=role3,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role3
uniqueMember: uid=jdoe,ou=People,dc=example,dc=com
""",
]

boost_tests = [
    'bytes_ostream_test',
    'chunked_vector_test',
    'compress_test',
    'continuous_data_consumer_test',
    'types_test',
    'keys_test',
    'mutation_test',
    'mvcc_test',
    'schema_registry_test',
    'range_test',
    ('mutation_reader_test', '-c{} -m2G'.format(min(os.cpu_count(), 3))),
    'serialized_action_test',
    'cdc_test',
    'cql_query_test',
    'user_types_test',
    'user_function_test',
    'secondary_index_test',
    'json_cql_query_test',
    'filtering_test',
    'storage_proxy_test',
    'schema_change_test',
    'sstable_mutation_test',
    'sstable_resharding_test',
    'incremental_compaction_test',
    'commitlog_test',
    'hash_test',
    'test-serialization',
    'cartesian_product_test',
    'allocation_strategy_test',
    'UUID_test',
    'compound_test',
    'murmur_hash_test',
    'partitioner_test',
    'frozen_mutation_test',
    'canonical_mutation_test',
    'gossiping_property_file_snitch_test',
    'row_cache_test',
    'cache_flat_mutation_reader_test',
    'network_topology_strategy_test',
    'query_processor_test',
    'batchlog_manager_test',
    'logalloc_test',
    'log_heap_test',
    'crc_test',
    'checksum_utils_test',
    'flush_queue_test',
    'config_test',
    'dynamic_bitset_test',
    'gossip_test',
    'managed_vector_test',
    'map_difference_test',
    'memtable_test',
    'mutation_query_test',
    'snitch_reset_test',
    'auth_test',
    'idl_test',
    'range_tombstone_list_test',
    'mutation_fragment_test',
    'flat_mutation_reader_test',
    'anchorless_list_test',
    'database_test',
    'input_stream_test',
    'nonwrapping_range_test',
    'virtual_reader_test',
    'counter_test',
    'cell_locker_test',
    'view_schema_test',
    'view_build_test',
    'view_complex_test',
    'clustering_ranges_walker_test',
    'vint_serialization_test',
    'duration_test',
    'loading_cache_test',
    'castas_fcts_test',
    'big_decimal_test',
    'aggregate_fcts_test',
    'caching_options_test',
    'auth_resource_test',
    'cql_auth_query_test',
    'enum_set_test',
    'extensions_test',
    'cql_auth_syntax_test',
    'querier_cache',
    'limiting_data_source_test',
    ('sstable_test', '-c1 -m2G'),
    ('sstable_datafile_test', '-c1 -m2G'),
    'broken_sstable_test',
    ('sstable_3_x_test', '-c1 -m2G'),
    'meta_test',
    'reusable_buffer_test',
    'mutation_writer_test',
    'observable_test',
    'transport_test',
    'fragmented_temporary_buffer_test',
    'encrypted_file_test',
    'auth_passwords_test',
    'multishard_mutation_query_test',
    'top_k_test',
    'utf8_test',
    'small_vector_test',
    'data_listeners_test',
    'truncation_migration_test',
    'symmetric_key_test',
    'like_matcher_test',
    'linearizing_input_stream_test',
    'enum_option_test',
]

other_tests = [
    'memory_footprint',
]

long_tests = [
    ('lsa_async_eviction_test', '-c1 -m200M --size 1024 --batch 3000 --count 2000000'),
    ('lsa_sync_eviction_test', '-c1 -m100M --count 10 --standard-object-size 3000000'),
    ('lsa_sync_eviction_test', '-c1 -m100M --count 24000 --standard-object-size 2048'),
    ('lsa_sync_eviction_test', '-c1 -m1G --count 4000000 --standard-object-size 128'),
    ('row_cache_alloc_stress', '-c1 -m2G'),
    ('row_cache_stress_test', '-c1 -m1G --seconds 10'),
]

ldap_tests = [
    'ldap_connection_test',
    'role_manager_test',
]

CONCOLORS = {'green': '\033[1;32m', 'red': '\033[1;31m', 'nocolor': '\033[0m'}

def colorformat(msg, **kwargs):
    fmt = dict(CONCOLORS)
    fmt.update(kwargs)
    return msg.format(**fmt)

def status_to_string(success):
    if success:
        status = colorformat("{green}PASSED{nocolor}") if os.isatty(sys.stdout.fileno()) else "PASSED"
    else:
        status = colorformat("{red}FAILED{nocolor}") if os.isatty(sys.stdout.fileno()) else "FAILED"

    return status

class UnitTest:
    standard_args = '--overprovisioned --unsafe-bypass-fsync 1 --blocked-reactor-notify-ms 2000000 --collectd 0'.split()
    seastar_args = '-c2 -m2G'

    def __init__(self, test_no, name, opts, kind, mode, options):
        if opts is None:
            opts = UnitTest.seastar_args
        self.id = test_no
        self.name = name
        self.mode = mode
        self.path = os.path.join('build', self.mode, 'tests', self.name)
        self.kind = kind
        self.args = opts.split() + UnitTest.standard_args

        if self.kind in ['boost', 'ldap']:
            boost_args = []
            if options.jenkins:
                mode = 'debug' if self.mode == 'debug' else 'release'
                xmlout = options.jenkins + "." + mode + "." + self.name + "." + str(self.id) + ".boost.xml"
                boost_args += ['--report_level=no', '--logger=HRF,test_suite:XML,test_suite,' + xmlout]
            boost_args += ['--']
            self.args = boost_args + self.args


def print_progress(test, success, cookie, verbose):
    if isinstance(cookie, int):
        cookie = (0, 1, cookie)

    last_len, n, n_total = cookie
    msg = "[{}/{}] {} {} {}".format(n, n_total, status_to_string(success), test.path, ' '.join(test.args))
    if verbose is False and sys.stdout.isatty():
        print('\r' + ' ' * last_len, end='')
        last_len = len(msg)
        print('\r' + msg, end='')
    else:
        print(msg)

    return (last_len, n + 1, n_total)


def setup(test, port, options):
    """Performs any necessary setup steps before running a test.

Returns (fn, txt) where fn is a cleanup function to call unconditionally after the test stops running, and txt is failure-injection description."""
    if test.kind == 'ldap':
        instances_root = os.path.join(os.path.split(test.path)[0], 'ldap_instances');
        instance_path = os.path.join(os.path.abspath(instances_root), str(port))
        slapd_pid_file = os.path.join(instance_path, 'slapd.pid')
        data_path = os.path.join(instance_path, 'data')
        os.makedirs(data_path)
        # This will always fail because it lacks the permissions to read the default slapd data
        # folder but it does create the instance folder so we don't want to fail here.
        try:
            subprocess.check_output(['slaptest', '-f', LDAP_SERVER_CONFIGURATION_FILE, '-F', instance_path],
                                    stderr=subprocess.DEVNULL)
        except:
            pass
        # Set up failure injection.
        proxy_name = 'p{}'.format(port)
        subprocess.check_output([
            'toxiproxy-cli', 'c', proxy_name,
            '--listen', 'localhost:{}'.format(port + 2), '--upstream', 'localhost:{}'.format(port)])
        # Sever the connection after byte_limit bytes have passed through:
        byte_limit = options.byte_limit if options.byte_limit else randint(0, 2000)
        subprocess.check_output(['toxiproxy-cli', 't', 'a', proxy_name, '-t', 'limit_data', '-n', 'limiter',
                                 '-a', 'bytes={}'.format(byte_limit)])
        # Change the data folder in the default config.
        replace_expression = 's/olcDbDirectory:.*/olcDbDirectory: {}/g'.format(
            os.path.abspath(data_path).replace('/','\/'))
        subprocess.check_output(
            ['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'])
        # Change the pid file to be kept with the instance.
        replace_expression = 's/olcPidFile:.*/olcPidFile: {}/g'.format(
            os.path.abspath(slapd_pid_file).replace('/', '\/'))
        subprocess.check_output(
            ['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'])
        # Put the test data in.
        cmd = ['slapadd', '-F', os.path.abspath(instance_path)]
        subprocess.check_output(
            cmd, input='\n\n'.join(DEFAULT_ENTRIES).encode('ascii'), stderr=subprocess.STDOUT)
        # Set up the server.
        SLAPD_URLS='ldap://:{}/ ldaps://:{}/'.format(port, port + 1)
        process = subprocess.Popen(['slapd', '-F', os.path.abspath(instance_path), '-h', SLAPD_URLS, '-d', '0'])
        time.sleep(0.25) # On some systems, slapd needs a second to become available.
        def finalize():
            process.terminate()
            shutil.rmtree(instance_path)
            subprocess.check_output(['toxiproxy-cli', 'd', proxy_name])
        return finalize, '--byte-limit={}'.format(byte_limit)
    else:
        return (lambda: 0, None)


async def run_test(test, options):
    file = io.StringIO()

    def report_error(out, failure_injection_desc = None):
        print('=== stdout START ===', file=file)
        print(out, file=file)
        print('=== stdout END ===', file=file)
        if failure_injection_desc is not None:
            print('failure injection: {}'.format(failure_injection_desc), file=file)
    success = False
    process = None
    stdout = None
    ldap_port = 5000 + test.id * 3
    cleanup_fn = None
    finject_desc = None
    try:
        cleanup_fn, finject_desc = setup(test, ldap_port, options)
        if not options.manual_execution:
            process = await asyncio.create_subprocess_exec(
                test.path,
                *test.args,
                stderr=asyncio.subprocess.STDOUT,
                stdout=asyncio.subprocess.PIPE,
                env=dict(os.environ,
                    UBSAN_OPTIONS='print_stacktrace=1',
                    BOOST_TEST_CATCH_SYSTEM_ERRORS='no',
                    SEASTAR_LDAP_PORT=str(ldap_port)),
                    preexec_fn=os.setsid,
                )
            stdout, _ = await asyncio.wait_for(process.communicate(), options.timeout)
            success = process.returncode == 0
            if process.returncode != 0:
                print('  with error code {code}\n'.format(code=process.returncode), file=file)
                report_error(stdout.decode(encoding='UTF-8'), finject_desc)
        else:
            print('Please run the following shell command, then press <enter>:')
            shcmd = ' '.join([shlex.quote(e) for e in [test.path, *test.args]])
            shcmd = 'SEASTAR_LDAP_PORT={} {}'.format(ldap_port, shcmd)
            print(shcmd)
            input('-- press <enter> to continue --')
            success = True
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        if process is not None:
            process.kill()
            stdout, _ = await process.communicate()
        if isinstance(e, asyncio.TimeoutError):
            print('  timed out', file=file)
            report_error(stdout.decode(encoding='UTF-8') if stdout else "No output", finject_desc)
        elif isinstance(e, asyncio.CancelledError):
            print(test.name, end=" ")
    except Exception as e:
        print('  with error {e}\n'.format(e=e), file=file)
        report_error(e, finject_desc)
    finally:
        if cleanup_fn is not None:
            cleanup_fn()
    return (test, success, file.getvalue())


def setup_signal_handlers(loop, signaled):

    async def shutdown(loop, signo, signaled):
        print("\nShutdown requested... Aborting tests:"),
        signaled.signo = signo
        signaled.set()

    # Use a lambda to avoid creating a coroutine until
    # the signal is delivered to the loop - otherwise
    # the coroutine will be dangling when the loop is over,
    # since it's never going to be invoked
    for signo in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(signo, lambda: asyncio.create_task(shutdown(loop, signo, signaled)))


def parse_cmd_line():
    """ Print usage and process command line options. """
    all_modes = ['debug', 'release', 'dev', 'sanitize']
    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 2e9
    cpus_per_test_job = 1
    default_num_jobs_mem = ((sysmem - 4e9) // testmem)
    default_num_jobs_cpu = multiprocessing.cpu_count() // cpus_per_test_job
    default_num_jobs = min(default_num_jobs_mem, default_num_jobs_cpu)

    parser = argparse.ArgumentParser(description="Scylla test runner")
    parser.add_argument('--name', action="store",
                        help="Run only test whose name contains given string")
    parser.add_argument('--mode', choices=all_modes, action="append", dest="modes",
                        help="Run only tests for given build mode(s)")
    parser.add_argument('--repeat', action="store", default="1", type=int,
                        help="number of times to repeat test execution")
    parser.add_argument('--timeout', action="store", default="3000", type=int,
                        help="timeout value for test execution")
    parser.add_argument('--jenkins', action="store",
                        help="jenkins output file prefix")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--jobs', '-j', action="store", default=default_num_jobs, type=int,
                        help="Number of jobs to use for running the tests")
    parser.add_argument('--xunit', action="store",
                        help="Name of a file to write results of non-boost tests to in xunit format")
    parser.add_argument('--manual-execution', action='store_true', default=False,
                        help='Let me manually run the test executable at the moment this script would run it')
    parser.add_argument('--byte-limit', action="store", default=None, type=int,
                        help="Specific byte limit for failure injection (random by default)")
    args = parser.parse_args()

    if not args.modes:
        out = subprocess.Popen(['ninja', 'mode_list'], stdout=subprocess.PIPE).communicate()[0].decode()
        # [1/1] List configured modes
        # debug release dev
        args.modes = out.split('\n')[1].split(' ')

    return args


def find_tests(options):

    tests_to_run = []

    for mode in options.modes:
        def add_test_list(lst, kind):
            for t in lst:
                tests_to_run.append((t, None, kind, mode) if isinstance(t, str) else (*t, kind, mode))

        add_test_list(other_tests, 'other')
        add_test_list(boost_tests, 'boost')
        add_test_list(ldap_tests, 'ldap')
        if mode in ['release', 'dev']:
            add_test_list(long_tests, 'other')

    if options.name:
        tests_to_run = [t for t in tests_to_run if options.name in t[0]]
        if not tests_to_run:
            print("Test {} not found".format(options.name))
            sys.exit(1)

    tests_to_run = [t for t in tests_to_run for _ in range(options.repeat)]
    tests_to_run = [UnitTest(test_no, *t, options) for test_no, t in enumerate(tests_to_run)]

    return tests_to_run


async def run_all_tests(tests_to_run, signaled, options):
    failed_tests = []
    results = []
    cookie = len(tests_to_run)
    signaled_task = asyncio.create_task(signaled.wait())
    pending = set([signaled_task])

    async def cancel(pending):
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        print("... done.")
        raise asyncio.CancelledError

    async def reap(done, pending, signaled):
        nonlocal cookie
        if signaled.is_set():
            await cancel(pending)
        for coro in done:
            result = coro.result()
            if isinstance(result, bool):
                continue # skip signaled task result
            results.append(result)
            test, success, out = result
            cookie = print_progress(test, success, cookie, options.verbose)
            if not success:
                failed_tests.append((test, out))
    try:
        for test in tests_to_run:
            # +1 for 'signaled' event
            if len(pending) > options.jobs:
                # Wait for some task to finish
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                await reap(done, pending, signaled)
            pending.add(asyncio.create_task(run_test(test, options)))
        # Wait & reap ALL tasks but signaled_task
        # Do not use asyncio.ALL_COMPLETED to print a nice progress report
        while len(pending) > 1:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            await reap(done, pending, signaled)

    except asyncio.CancelledError:
        return None, None

    return failed_tests, results


def print_summary(failed_tests, total_tests):
    if not failed_tests:
        print('\nOK.')
    else:
        print('\n\nOutput of the failed tests:')
        for test, out in failed_tests:
            print("Test {} {} failed:\n{}".format(test.path, ' '.join(test.args), out))
        print('\n\nThe following test(s) have failed:')
        for test, _ in failed_tests:
            print('  {} {}'.format(test.path, ' '.join(test.args)))
        print('\nSummary: {} of the total {} tests failed'.format(len(failed_tests), total_tests))

def write_xunit_report(options, results):
    other_results = [r for r in results if r[0].kind != 'boost']
    num_other_failed = sum(1 for r in other_results if not r[1])

    xml_results = ET.Element('testsuite', name='non-boost tests',
            tests=str(len(other_results)), failures=str(num_other_failed), errors='0')

    for test, success, out in other_results:
        xml_res = ET.SubElement(xml_results, 'testcase', name=test.path)
        if not success:
            xml_fail = ET.SubElement(xml_res, 'failure')
            xml_fail.text = "Test {} {} failed:\n{}".format(test.path, ' '.join(test.args), out)
    with open(options.xunit, "w") as f:
        ET.ElementTree(xml_results).write(f, encoding="unicode")

async def main():

    options = parse_cmd_line()

    tests_to_run = find_tests(options)
    if options.manual_execution and len(tests_to_run) > 1:
        print('--manual-execution only supports running a single test, but multiple selected: {}'.format(
            [e.path for e in tests_to_run[:3]])) # Print the whole e.path, as matches may come from multiple dirs.
        sys.exit(1)
    signaled = asyncio.Event()

    setup_signal_handlers(asyncio.get_event_loop(), signaled)

    tp_server = None
    try:
        if [t for t in tests_to_run if t.kind == 'ldap']:
            tp_server = subprocess.Popen('toxiproxy-server', stderr=subprocess.DEVNULL)
        failed_tests, results = await run_all_tests(tests_to_run, signaled, options)
    finally:
        if tp_server is not None:
            tp_server.terminate()

    if signaled.is_set():
        return -signaled.signo

    print_summary(failed_tests, len(tests_to_run))

    if options.xunit:
        write_xunit_report(options, results)
    return 0

if __name__ == "__main__":
    if sys.version_info < (3, 7):
        print("Python 3.7 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(main()))
