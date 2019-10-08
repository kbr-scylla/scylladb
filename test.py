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
import os
import sys
import argparse
import subprocess
import concurrent.futures
import io
import multiprocessing
import xml.etree.ElementTree as ET
import shutil
import signal
import shlex
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
"""
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
    'mutation_reader_test',
    'serialized_action_test',
    'cdc_test',
    'cql_query_test',
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
    'role_manager_test',
    'caching_options_test',
    'auth_resource_test',
    'cql_auth_query_test',
    'enum_set_test',
    'extensions_test',
    'cql_auth_syntax_test',
    'querier_cache',
    'limiting_data_source_test',
    'sstable_test',
    'sstable_datafile_test',
    'broken_sstable_test',
    'sstable_3_x_test',
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
]

other_tests = [
    'memory_footprint',
]

ldap_tests = [
    'ldap_connection_test',
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

def print_progress_succint(test_path, test_args, success, cookie):
    if type(cookie) is int:
        cookie = (0, 1, cookie)

    last_len, n, n_total = cookie
    msg = "[{}/{}] {} {} {}".format(n, n_total, status_to_string(success), test_path, ' '.join(test_args))
    if sys.stdout.isatty():
        print('\r' + ' ' * last_len, end='')
        last_len = len(msg)
        print('\r' + msg, end='')
    else:
        print(msg)

    return (last_len, n + 1, n_total)


def print_status_verbose(test_path, test_args, success, cookie):
    if type(cookie) is int:
        cookie = (1, cookie)

    n, n_total = cookie
    msg = "[{}/{}] {} {} {}".format(n, n_total, status_to_string(success), test_path, ' '.join(test_args))
    print(msg)

    return (n + 1, n_total)


class Alarm(Exception):
    pass


def alarm_handler(signum, frame):
    raise Alarm


if __name__ == "__main__":
    all_modes = ['debug', 'release', 'dev', 'sanitize']

    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 2e9
    cpus_per_test_job = 1
    default_num_jobs_mem = ((sysmem - 4e9) // testmem)
    default_num_jobs_cpu = multiprocessing.cpu_count() // cpus_per_test_job
    default_num_jobs = min(default_num_jobs_mem, default_num_jobs_cpu)

    parser = argparse.ArgumentParser(description="Scylla test runner")
    parser.add_argument('--fast', action="store_true",
                        help="Run only fast tests")
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

    print_progress = print_status_verbose if args.verbose else print_progress_succint

    custom_seastar_args = {
        "sstable_test": ['-c1', '-m2G'],
        'sstable_datafile_test': ['-c1', '-m2G'],
        "sstable_3_x_test": ['-c1', '-m2G'],
        "mutation_reader_test": ['-c{}'.format(min(os.cpu_count(), 3)), '-m2G'],
    }

    test_to_run = []
    modes_to_run =  ['debug', 'release', 'dev'] if not args.modes else args.modes
    for mode in modes_to_run:
        prefix = os.path.join('build', mode, 'tests')
        standard_args = '--overprovisioned --unsafe-bypass-fsync 1 --blocked-reactor-notify-ms 2000000'.split()
        seastar_args = '-c2 -m2G'.split()
        for test in other_tests:
            test_to_run.append((os.path.join(prefix, test), 'other', custom_seastar_args.get(test, seastar_args) + standard_args))
        for test in boost_tests:
            test_to_run.append((os.path.join(prefix, test), 'boost', custom_seastar_args.get(test, seastar_args) + standard_args))
        for test in ldap_tests:
            test_to_run.append((os.path.join(prefix, test), 'ldap', custom_seastar_args.get(test, seastar_args) + standard_args))

    for m in ['release', 'dev']:
        if m in modes_to_run:
            test_to_run.append(('build/' + m + '/tests/lsa_async_eviction_test', 'other',
                                '-c1 -m200M --size 1024 --batch 3000 --count 2000000'.split() + standard_args))
            test_to_run.append(('build/' + m + '/tests/lsa_sync_eviction_test', 'other',
                                '-c1 -m100M --count 10 --standard-object-size 3000000'.split() + standard_args))
            test_to_run.append(('build/' + m + '/tests/lsa_sync_eviction_test', 'other',
                                '-c1 -m100M --count 24000 --standard-object-size 2048'.split() + standard_args))
            test_to_run.append(('build/' + m + '/tests/lsa_sync_eviction_test', 'other',
                                '-c1 -m1G --count 4000000 --standard-object-size 128'.split() + standard_args))
            test_to_run.append(('build/' + m + '/tests/row_cache_alloc_stress', 'other',
                                '-c1 -m2G'.split() + standard_args))
            test_to_run.append(('build/' + m + '/tests/row_cache_stress_test', 'other', '-c1 -m1G --seconds 10'.split() + standard_args))

    if args.name:
        test_to_run = [t for t in test_to_run if args.name in t[0]]
        if not test_to_run:
            print("Test {} not found".format(args.name))
            sys.exit(1)

    failed_tests = []

    n_total = len(test_to_run)
    if args.manual_execution and n_total > 1:
        print('--manual-execution only supports running a single test, but multiple selected: {}'.format(
            [e[0] for e in test_to_run[:3]])) # Print the whole e[0] path, as matches may come from multiple dirs.
        sys.exit(1)
    env = os.environ
    env['UBSAN_OPTIONS'] = 'print_stacktrace=1'
    env['BOOST_TEST_CATCH_SYSTEM_ERRORS'] = 'no'

    def noop_setup():
        yield None, None
    port = 5000

    def ldap_setup(ldap_path):
        global port

        local_port = 0
        local_port_ldap = port
        local_port_sldap = port + 1
        local_port_failure_injection = port + 2
        port = port + 3
        slapd_pid = None
        byte_limit = args.byte_limit if args.byte_limit else randint(0, 2000)
        # Start creating the instance folder structure.
        instance_path = os.path.join(os.path.abspath(ldap_path),str(local_port_ldap))
        slapd_pid_file = os.path.join(instance_path,"slapd.pid")
        if os.path.exists(instance_path):
            # Kill the slapd process if it happen to be a residual one this really should not happen
            # unless in the extreme case a previous test.py process crashing in the middle of an
            # ldap test.
            if os.path.exists(slapd_pid_file):
                with open(slapd_pid_file, 'r') as pidfile:
                    slapd_pid = int(pidfile.read())
                    os.kill(slapd_pid,signal.SIGTERM)
                    slapd_pid = None
            shutil.rmtree(instance_path)
        data_path = os.path.join(instance_path,'data')
        os.makedirs(data_path)
        # This will always fail because it lacks the permissions to read the default slapd data
        # folder but it does create the instance folder so we don't want to fail here.
        try:
            with open(os.devnull, 'w') as devnull:
                subprocess.check_output(['slaptest','-f',LDAP_SERVER_CONFIGURATION_FILE,'-F',instance_path],stderr=devnull)
        except:
            pass
        # Set up failure injection.
        proxy_name = 'p{}'.format(local_port_ldap)
        subprocess.check_output(['toxiproxy-cli', 'c', proxy_name, '--listen',
                                 'localhost:{}'.format(local_port_failure_injection),
                                 '--upstream', 'localhost:{}'.format(local_port_ldap)])
        # Sever the connection after byte_limit bytes have passed through:
        subprocess.check_output(['toxiproxy-cli', 't', 'a', proxy_name, '-t', 'limit_data', '-n', 'limiter',
                                 '-a', 'bytes={}'.format(byte_limit)])
        # Change the data folder in the default config.
        replace_expression = 's/olcDbDirectory:.*/olcDbDirectory: {}/g'.format(os.path.abspath(data_path).replace('/','\/'))
        cmd = ['find',instance_path,'-type','f','-exec','sed','-i',replace_expression,'{}',";"]
        subprocess.check_output(cmd)
        # Change the pid file to be kept with the instance.
        replace_expression = 's/olcPidFile:.*/olcPidFile: {}/g'.format(os.path.abspath(slapd_pid_file).replace('/','\/'))
        cmd = ['find',instance_path,'-type','f','-exec','sed','-i',replace_expression,'{}',";"]
        subprocess.check_output(cmd)
        # Put the test data in.
        cmd = ['slapadd','-F',os.path.abspath(instance_path)]
        subprocess.check_output(
            cmd, input='\n\n'.join(DEFAULT_ENTRIES).encode('ascii'), stderr=subprocess.STDOUT)
        # Set up the server.
        SLAPD_URLS="ldap://:{local_port_ldap}/ ldaps://:{local_port_sldap}/".format(local_port_ldap=local_port_ldap,local_port_sldap=local_port_sldap)
        subprocess.check_output(['slapd','-F',os.path.abspath(instance_path),'-h',SLAPD_URLS,'-n',"server-{}".format(local_port_ldap)])
        # Record the process pid.
        with open(slapd_pid_file, 'r') as pidfile:
            slapd_pid = int(pidfile.read())
        # Put some data in.
        yield local_port_ldap, byte_limit

        # Wrap up logic - this will be executed when this generator is enumerated.

        # Kill the slapd process.
        os.kill(slapd_pid,signal.SIGTERM)
        # Remove test directory.
        shutil.rmtree(instance_path)
        # Clean up failure injection.
        subprocess.check_output(['toxiproxy-cli', 'd', proxy_name])

    def run_test(path, type, exec_args, setup):
        boost_args = []
        # avoid modifying in-place, it will change test_to_run
        exec_args = exec_args + '--collectd 0'.split()
        file = io.StringIO()
        if args.jenkins and type == 'boost':
            mode = 'release'
            if path.startswith(os.path.join('build', 'debug')):
                mode = 'debug'
            xmlout = (args.jenkins + "." + mode + "." + os.path.basename(path.split()[0]) + ".boost.xml")
            boost_args += ['--report_level=no', '--logger=HRF,test_suite:XML,test_suite,' + xmlout]
        if type in ['boost', 'ldap']:
            boost_args += ['--']

        def report_error(exc, out, report_subcause, failure_injection_desc):
            report_subcause(exc)
            if out:
                print('=== stdout START ===', file=file)
                print(out, file=file)
                print('=== stdout END ===', file=file)
            if failure_injection_desc:
                print('failure injection: {}'.format(failure_injection_desc), file=file)
        success = False
        failure_injection_desc = None
        try:
            for (port, byte_limit) in setup():
                if type == 'ldap':
                    env['SEASTAR_LDAP_PORT'] = str(port)
                    failure_injection_desc = '--byte-limit={}'.format(byte_limit)
                cmd = [path] + boost_args + exec_args
                if not args.manual_execution:
                    subprocess.check_output(
                        cmd, stderr=subprocess.STDOUT, timeout=args.timeout, env=env, preexec_fn=os.setsid)
                else:
                    print('Please run the following shell command, then press <enter>:')
                    shcmd = ' '.join([shlex.quote(e) for e in cmd])
                    if type == 'ldap':
                        shcmd = 'SEASTAR_LDAP_PORT={} {}'.format(port, shcmd)
                    print(shcmd)
                    input('-- press <enter> to continue --')
                success = True
        # TODO: if an exception is thrown, the setup() enumeration terminates early, so some cleanup
        # inside it is skipped.  Should be handled below.
        except subprocess.TimeoutExpired as e:
            def report_subcause(e):
                print('  timed out', file=file)
            report_error(e, e.output.decode(encoding='UTF-8'), report_subcause, failure_injection_desc)
        except subprocess.CalledProcessError as e:
            def report_subcause(e):
                print('  with error code {code}\n'.format(code=e.returncode), file=file)
            report_error(e, e.output.decode(encoding='UTF-8'), report_subcause, failure_injection_desc)
        except Exception as e:
            def report_subcause(e):
                print('  with error {e}\n'.format(e=e), file=file)
            report_error(e, e, report_subcause, failure_injection_desc)
        return (path, boost_args + exec_args, type, success, file.getvalue())

    with open(os.devnull, 'w') as devnull:
        tp_server = subprocess.Popen('toxiproxy-server', stderr=devnull)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs)
    futures = []
    # We take note of all slapd processes that ran prior to the tests in order not to clean them.
    slapd_before = set([])
    try:
        slapd_before = set(map(int,subprocess.check_output(["pidof",'slapd']).split()))
    except:
        pass
    instance_dirs_to_remove = set([])
    for n, test in enumerate(test_to_run):
        path = test[0]
        test_path = os.path.split(path)[0];
        ldap_path = os.path.join(test_path,'ldap_instances');
        setup = noop_setup
        test_type = test[1]
        if test_type == 'ldap':
            instance_dirs_to_remove.add(ldap_path)
            setup = lambda: ldap_setup(ldap_path)
        exec_args = test[2] if len(test) >= 3 else []
        for _ in range(args.repeat):
            futures.append(executor.submit(run_test, path, test_type, exec_args, setup))

    results = []
    cookie = len(futures)
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        results.append(result)
        test_path, test_args, _, success, out = result
        cookie = print_progress(test_path, test_args, success, cookie)
        if not success:
            failed_tests.append((test_path, test_args, out))
    # After all test are done we need to clean the residual slapds if there are any.  The correct
    # state here is not to have residual slapds since each test cleans after itself.  This is just
    # another measurement taken in order not to pollute the environment we run in.
    slapd_after = set([])
    try:
        slapd_after = set(map(int,subprocess.check_output(["pidof",'slapd']).split()))
    except:
        pass
    residual_slapds = slapd_after.difference(slapd_before)
    for pid in residual_slapds:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    # Remove all residual folders of slapd.
    for instance_dir in  instance_dirs_to_remove:
        shutil.rmtree(instance_dir)

    tp_server.terminate()

    if not failed_tests:
        print('\nOK.')
    else:
        print('\n\nOutput of the failed tests:')
        for test, test_args, out in failed_tests:
            print("Test {} {} failed:\n{}".format(test, ' '.join(test_args), out))
        print('\n\nThe following test(s) have failed:')
        for test, test_args, _ in failed_tests:
            print('  {} {}'.format(test, ' '.join(test_args)))
        print('\nSummary: {} of the total {} tests failed'.format(len(failed_tests), len(results)))

    if args.xunit:
        other_results = [r for r in results if r[2] != 'boost']
        num_other_failed = sum(1 for r in other_results if not r[3])

        xml_results = ET.Element('testsuite', name='non-boost tests',
                tests=str(len(other_results)), failures=str(num_other_failed), errors='0')

        for test_path, test_args, _, success, out in other_results:
            xml_res = ET.SubElement(xml_results, 'testcase', name=test_path)
            if not success:
                xml_fail = ET.SubElement(xml_res, 'failure')
                xml_fail.text = "Test {} {} failed:\n{}".format(test_path, ' '.join(test_args), out)
        with open(args.xunit, "w") as f:
            ET.ElementTree(xml_results).write(f, encoding="unicode")

    if failed_tests:
        sys.exit(1)
