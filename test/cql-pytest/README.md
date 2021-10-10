Single-node functional tests for Scylla's CQL features.

These tests use the Python CQL library and the pytest frameworks.
By using an actual CQL library for the tests, they can be run against *any*
implementation of CQL - both Scylla and Cassandra. Most tests - except in
rare cases - should pass on both, to ensure that Scylla is compatible with
Cassandra in most features.

To run all tests against an already-running local installation of Scylla
or Cassandra on localhost, just run `pytest`. The "--host" and "--port"
can be used to give a different location for the running Scylla or Cassanra.
The "--ssl" option can be used to use an encrypted (TLSv1.2) connection.

More conveniently, we have two scripts - "run" and "run-cassandra" - which
do all the work necessary to start Scylla or Cassandra (respectively),
and run the tests on them. The Scylla or Cassandra process is run in a
temporary directory which is automatically deleted when the test ends.

"run" automatically picks the most recently compiled version of Scylla in
`build/*/scylla` - but this choice of Scylla executable can be overridden with
the `SCYLLA` environment variable. "run-cassandra" defaults to running the
command `cassandra` from the user's path, but this can be overriden by setting
the `CASSANDRA` environment variable to the path of the `cassandra` script,
e.g., `export CASSANDRA=$HOME/apache-cassandra-3.11.10/bin/cassandra`.
A few of the tests also require the `nodetool` when running on Cassandra -
this tool is again expected to be in the user's path, or be overridden with
the `NODETOOL` environment variable. Nodetool is **not** needed to test
Scylla.

Additional options can be passed to "pytest" or to "run" / "run-cassandra"
to control which tests to run:

* To run all tests in a single file, do `pytest test_table.py`.
* To run a single specific test, do `pytest test_table.py::test_create_table_unsupported_names`.
* To run the same test or tests 100 times, add the `--count=100` option.
  This is faster than running `run` 100 times, because Scylla is only run
  once, and also counts for you how many of the runs failed.
  For `pytest` to support the `--count` option, you need to install a
  pytest extension: `pip install pytest-repeat`

Additional useful pytest options, especially useful for debugging tests:

* -v: show the names of each individual test running instead of just dots.
* -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)
