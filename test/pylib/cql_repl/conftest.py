#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#


def pytest_addoption(parser) -> None:
    parser.addoption("--input", action="store", default="",
                     help="Input file")
    parser.addoption("--output", action="store", default="",
                     help="Output file")
