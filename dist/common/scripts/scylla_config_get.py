#!/usr/bin/python2
#
# Copyright 2016 ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

import sys
import yaml
import argparse

def get(config, key):
    s = open(config).read()
    cfg = yaml.load(s)
    try:
        val = cfg[key]
    except KeyError:
        print("key '%s' not found" % key)
        sys.exit(1)
    if isinstance(val, list):
        for v in val:
            print("%s" % v)
    elif isinstance(val, dict):
        for k, v in list(val.items()):
            print("%s:%s" % (k, v))
    else:
        print(val)

def main():
    parser = argparse.ArgumentParser(description='scylla.yaml config reader/writer from shellscript.')
    parser.add_argument('-c', '--config', dest='config', action='store',
                        default='/etc/scylla/scylla.yaml',
                        help='path to scylla.yaml')
    parser.add_argument('-g', '--get', dest='get', action='store',
                        required=True, help='get parameter')
    args = parser.parse_args()
    get(args.config, args.get)

if __name__ == "__main__":
    main()
