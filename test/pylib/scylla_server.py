#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#
import aiohttp
import asyncio
import logging
import os
import pathlib
import shutil
import time
import uuid
from typing import Optional, List, Callable
from cassandra import InvalidRequest                    # type: ignore
from cassandra.auth import PlainTextAuthProvider        # type: ignore
from cassandra.cluster import Cluster, NoHostAvailable  # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore
from cassandra.policies import RoundRobinPolicy                          # type: ignore

#
# Put all Scylla options in a template file. Sic: if you make a typo in the
# configuration file, Scylla will boot fine and ignore the setting.
# Always check the error log after modifying the template.
#
SCYLLA_CONF_TEMPLATE = """cluster_name: {cluster_name}
developer_mode: true

# Allow testing experimental features. Following issue #9467, we need
# to add here specific experimental features as they are introduced.

enable_user_defined_functions: true
experimental: true
experimental_features:
    - udf
#    - raft

data_file_directories:
    - {workdir}/data
commitlog_directory: {workdir}/commitlog
hints_directory: {workdir}/hints
view_hints_directory: {workdir}/view_hints

listen_address: {host}
rpc_address: {host}
api_address: {host}
prometheus_address: {host}
alternator_address: {host}

seed_provider:
    - class_name: org.apache.cassandra.locator.simple_seed_provider
      parameters:
          - seeds: {seeds}

skip_wait_for_gossip_to_settle: 0
ring_delay_ms: 0
num_tokens: 16
flush_schema_tables_after_modification: false
auto_snapshot: false

# Significantly increase default timeouts to allow running tests
# on a very slow setup (but without network losses). Note that these
# are server-side timeouts: The client should also avoid timing out
# its own requests - for this reason we increase the CQL driver's
# client-side timeout in conftest.py.

range_request_timeout_in_ms: 300000
read_request_timeout_in_ms: 300000
counter_write_request_timeout_in_ms: 300000
cas_contention_timeout_in_ms: 300000
truncate_request_timeout_in_ms: 300000
write_request_timeout_in_ms: 300000
request_timeout_in_ms: 300000

# Set up authentication in order to allow testing this module
# and other modules dependent on it: e.g. service levels

authenticator: PasswordAuthenticator
strict_allow_filtering: true
"""

# Seastar options can not be passed through scylla.yaml, use command line
# for them. Keep everything else in the configuration file to make
# it easier to restart. Sic: if you make a typo on the command line,
# Scylla refuses to boot.
SCYLLA_CMDLINE_OPTIONS = [
    '--smp', '2',
    '-m', '1G',
    '--collectd', '0',
    '--overprovisioned',
    '--max-networking-io-control-blocks', '100',
    '--unsafe-bypass-fsync', '1',
    '--kernel-page-cache', '1',
]


class ScyllaServer:
    def __init__(self, exe: str, vardir: str,
                 host_registry,
                 cluster_name: str, seed: str,
                 cmdline_options: List[str]) -> None:
        self.exe = pathlib.Path(exe).resolve()
        self.vardir = pathlib.Path(vardir)
        self.host_registry = host_registry
        self.cmdline_options = cmdline_options
        self.cluster_name = cluster_name
        self.hostname = ""
        self.seeds = seed
        self.cmd: Optional[asyncio.subprocess.Process] = None
        self.log_savepoint = 0

        async def stop_server() -> None:
            if self.is_running:
                await self.stop()

        async def uninstall_server() -> None:
            await self.uninstall()

        self.stop_artifact = stop_server
        self.uninstall_artifact = uninstall_server

    async def install_and_start(self) -> None:
        await self.install()

        logging.info("starting server at host %s...", self.hostname)

        await self.start()

        if self.cmd:
            logging.info("started server at host %s, pid %d", self.hostname, self.cmd.pid)

    @property
    def is_running(self) -> bool:
        return self.cmd is not None

    @property
    def host(self) -> str:
        return str(self.hostname)

    def find_scylla_executable(self) -> None:
        if not os.access(self.exe, os.X_OK):
            raise RuntimeError("{} is not executable", self.exe)

    async def install(self) -> None:
        """Create a working directory with all subdirectories, initialize
        a configuration file."""

        self.find_scylla_executable()

        # Scylla assumes all instances of a cluster use the same port,
        # so each instance needs an own IP address.
        self.hostname = await self.host_registry.lease_host()
        if not self.seeds:
            self.seeds = self.hostname
        # Use the last part in host IP 127.151.3.27 -> 27
        # There can be no duplicates within the same test run
        # thanks to how host registry registers subnets, and
        # different runs use different vardirs.
        shortname = pathlib.Path("scylla-" + self.host.split(".")[-1])
        self.workdir = self.vardir / shortname

        logging.info("installing Scylla server in %s...", self.workdir)

        self.log_filename = self.vardir / shortname.with_suffix(".log")

        self.config_filename = self.workdir / "conf/scylla.yaml"

        # Delete the remains of the previous run

        # Cleanup any remains of the previously running server in this path
        shutil.rmtree(self.workdir, ignore_errors=True)

        self.workdir.mkdir(parents=True, exist_ok=True)
        self.config_filename.parent.mkdir(parents=True, exist_ok=True)
        # Create a configuration file.
        fmt = {
              "cluster_name": self.cluster_name,
              "host": self.hostname,
              "seeds": self.seeds,
              "workdir": self.workdir,
        }
        with self.config_filename.open('w') as config_file:
            config_file.write(SCYLLA_CONF_TEMPLATE.format(**fmt))

        self.log_file = self.log_filename.open("wb")

    def take_log_savepoint(self) -> None:
        """Save the server current log size when a test starts so that if
        the test fails, we can only capture the relevant lines of the log"""
        self.log_savepoint = self.log_file.tell()

    def read_log(self) -> str:
        """ Return first 3 lines of the log + everything that happened
        since the last savepoint. Used to diagnose CI failures, so
        avoid a nessted exception."""
        try:
            with self.log_filename.open("r") as log:
                # Read the first 5 lines of the start log
                lines: List[str] = []
                for i in range(3):
                    lines.append(log.readline())
                # Read the lines since the last savepoint
                if self.log_savepoint and self.log_savepoint > log.tell():
                    log.seek(self.log_savepoint)
                return "".join(lines + log.readlines())
        except Exception as e:
            return "Exception when reading server log {}: {}".format(self.log_filename, str(e))

    async def cql_is_up(self) -> bool:
        """Test that CQL is serving (a check we use at start up)."""
        caslog = logging.getLogger('cassandra')
        oldlevel = caslog.getEffectiveLevel()
        # Be quiet about connection failures.
        caslog.setLevel('CRITICAL')
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        try:
            with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                         contact_points=[self.hostname], auth_provider=auth) as cluster:
                with cluster.connect() as session:
                    # auth::standard_role_manager creates "cassandra" role in an
                    # async loop auth::do_after_system_ready(), which retries
                    # role creation with an exponential back-off. In other
                    # words, even after CQL port is up, Scylla may still be
                    # initializing. When the role is ready, queries begin to
                    # work, so rely on this "side effect".
                    session.execute("CREATE KEYSPACE k WITH REPLICATION = {" +
                                    "'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                    session.execute("DROP KEYSPACE k")
                    self.control_connection = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                                                      contact_points=[self.hostname], auth_provider=auth).connect()
                    return True
        except (NoHostAvailable, InvalidRequest):
            return False
        finally:
            caslog.setLevel(oldlevel)
        # Any other exception may indicate a problem, and is passed to the caller.

    async def rest_api_is_up(self) -> bool:
        """Test that the Scylla REST API is serving. Can be used as a
        checker function at start up."""
        try:
            async with aiohttp.ClientSession() as s:
                url = "http://{}:10000/".format(self.hostname)
                async with s.get(url):
                    return True
        except aiohttp.ClientConnectionError:
            return False
        # Any other exception may indicate a problem, and is passed to the caller.

    async def start(self) -> None:
        """Start an installed server. May be used for restarts."""
        START_TIMEOUT = 300     # seconds

        # Add suite-specific command line options
        scylla_args = SCYLLA_CMDLINE_OPTIONS + self.cmdline_options
        env = os.environ.copy()
        env.clear()     # pass empty env to make user user's SCYLLA_HOME has no impact
        self.cmd = await asyncio.create_subprocess_exec(
            self.exe,
            *scylla_args,
            cwd=self.workdir,
            stderr=self.log_file,
            stdout=self.log_file,
            env=env,
            preexec_fn=os.setsid,
        )

        self.start_time = time.time()

        while time.time() < self.start_time + START_TIMEOUT:
            if self.cmd.returncode:
                with self.log_filename.open('r') as log_file:
                    logging.error("failed to start server at host %s", self.hostname)
                    logging.error("last line of {}:".format(self.log_filename))
                    log_file.seek(0, 0)
                    logging.error(log_file.readlines()[-1].rstrip())
                    h = logging.getLogger().handlers[0]
                    logpath = h.baseFilename if hasattr(h, 'baseFilename') else "?"  # type: ignore
                    raise RuntimeError("""Failed to start server at host {}.
Check the log files:
{}
{}""".format(self.hostname, logpath, self.log_filename))

            if await self.rest_api_is_up():
                if await self.cql_is_up():
                    return

            # Sleep 10 milliseconds and retry
            await asyncio.sleep(0.1)
            if self.seeds != self.hostname:
                await self.force_schema_migration()

        raise RuntimeError("failed to start server {}, check server log at {}".format(
            self.host, self.log_filename))

    async def force_schema_migration(self) -> None:
        """This is a hack to change schema hash on an existing cluster node
        which triggers a gossip round and propagation of entire application
        state. Helps quickly propagate tokens and speed up node boot if the
        previous state propagation was missed."""
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        with Cluster(contact_points=[self.seeds], auth_provider=auth) as cluster:
            with cluster.connect() as session:
                session.execute("CREATE KEYSPACE k WITH REPLICATION = {" +
                                "'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                session.execute("DROP KEYSPACE k")

    async def stop(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGKILL to
        stop, so is not graceful. Waits for the process to exit before return."""
        # Preserve for logging
        hostname = self.hostname
        logging.info("stopping server at host %s", hostname)
        if not self.cmd:
            return

        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("stopped server at host %s", hostname)
            self.cmd = None
            self.control_connection = None

    async def uninstall(self) -> None:
        """Clear all files left from a stopped server, including the
        data files and log files."""

        if not self.hostname:
            return
        logging.info("Uninstalling server at %s", self.workdir)

        shutil.rmtree(self.workdir)
        self.log_filename.unlink(missing_ok=True)

        await self.host_registry.release_host(self.hostname)
        self.hostname = ""

    def write_log_marker(self, msg) -> None:
        self.log_file.seek(0, 2)  # seek to file end
        self.log_file.write(msg.encode())
        self.log_file.flush()


class ScyllaCluster:
    def __init__(self, replicas: int,
                 create_server: Callable[[str, Optional[str]], ScyllaServer]) -> None:
        self.name = str(uuid.uuid1())
        self.replicas = replicas
        self.cluster: List[ScyllaServer] = []
        self.create_server = create_server
        self.start_exception: Optional[Exception] = None
        self.keyspace_count = 0

    async def install_and_start(self) -> None:
        try:
            for i in range(self.replicas):
                seed = self.cluster[-1].host if self.cluster else None
                server = self.create_server(self.name, seed)
                self.cluster.append(server)
                await server.install_and_start()
            self.keyspace_count = self._get_keyspace_count()
        except (RuntimeError, NoHostAvailable, InvalidRequest) as e:
            # If start fails, swallow the error to throw later,
            # at test time.
            self.start_exception = e

    def __getitem__(self, i: int) -> ScyllaServer:
        return self.cluster[i]

    def _get_keyspace_count(self) -> int:
        """Get the current keyspace count"""
        assert(self.start_exception is None)
        rows = self.cluster[0].control_connection.execute(
            "select count(*) as c from system_schema.keyspaces")
        keyspace_count = int(rows.one()[0])
        return keyspace_count

    def before_test(self, name) -> None:
        """Check that  the cluster is ready for a test. If
        there was a start error, throw it here - the server is
        started when it's added to the pool, which can't be attributed
        to any specific test, throwing it here would stop a specific
        test."""
        if self.start_exception:
            raise self.start_exception

        for server in self.cluster:
            server.write_log_marker("------ Starting test {} ------\n".format(name))

    def after_test(self, name) -> None:
        """Check that the cluster is still alive and the test
        hasn't left any garbage."""
        assert(self.start_exception is None)
        if self._get_keyspace_count() != self.keyspace_count:
            raise RuntimeError("Test post-condition failed, "
                               "the test must drop all keyspaces it creates.")
        for server in self.cluster:
            server.write_log_marker("------ Ending test {} ------\n".format(name))
