#!/usr/bin/env python2
#
# Copyright (C) 2017 ScyllaDB
#

#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
#
import threading
import datetime
import json
import requests
import subprocess
import argparse
import sys
import time
import os
import multiprocessing

try:
    from tinydb import TinyDB, Query
except:
    print "Some dependencies are needed by this tool, install with:"
    print "   sudo yum install python-pip || sudo apt-get install python-pip"
    print "   sudo pip install tinydb"
    sys.exit(-1)

class murmur3_partitioner:
    def __init__(self, api_host, msb=0, nr_shards=16, timeout=10):
        self.msb = msb
        self.nr_shards = nr_shards
        self.shard_start_list = []
        self.init_zero_based_shard_start()
        self.api_host = api_host
        self.timeout = timeout
        self.host_id = self.get_host_id()
        self.node_ip = self.get_node_ip()
        self.local_dc_name = self.get_local_dc_name()

    def get_node_ip(self):
        url = 'http://{}:10000/storage_service/host_id'.format(self.api_host)
        text = ''
        try:
            r = requests.get(url=url, timeout=self.timeout)
            text = r.text
            r.raise_for_status()
            data = json.loads(text)
            for d in data:
                if d['value'] == self.host_id:
                    return d['key']
            print "Error: can not get the ip address for host_id {}".format(self.host_id)
            sys.exit(-1)
        except Exception as e:
            print "Error: API failed: {}".format(e, text)
            sys.exit(-1)

    def get_host_id(self):
        url = 'http://{}:10000/storage_service/hostid/local'.format(self.api_host)
        text = ''
        try:
            r = requests.get(url=url, timeout=self.timeout)
            text = r.text
            r.raise_for_status()
            return text.strip('"')
        except Exception as e:
            print "Error: API failed: {}".format(e, text)
            sys.exit(-1)

    def get_local_dc_name(self):
        url = 'http://{}:10000/snitch/datacenter'.format(self.api_host)
        text = ''
        try:
            r = requests.get(url=url, timeout=self.timeout)
            text = r.text
            r.raise_for_status()
            return text.strip('"')
        except Exception as e:
            print "Error: API failed: {}".format(e, text)
            sys.exit(-1)

    def minimum_token(self):
        return -2**63

    def maximum_token(self):
        # FIXME: 2**63 - 1 is not used. -2**63 is the minimum token.
        return 2**63 -1

    def zero_based_shard_of(self, token):
        return (((token << self.msb) & (2**64-1)) * self.nr_shards) >> 64

    def shard_of(self, token):
        n = token + 2**63
        return self.zero_based_shard_of(n)

    def init_zero_based_shard_start(self):
        if self.nr_shards == 1:
            self.shard_start_list = [0]
        self.shard_start_list = [0] * self.nr_shards
        for s in xrange(self.nr_shards):
            token = (s << 64) / self.nr_shards
            token = token >> self.msb
            while (self.zero_based_shard_of(token) != s):
                token = token + 1
            self.shard_start_list[s] = token

    def get_shard_start(self, shard):
        token = self.shard_start_list[shard]
        return token

    def token_for_next_shard(self, token, shard, spans = 1):
        n = token + 2**63
        s = self.zero_based_shard_of(n)
        if msb == 0:
            n = self.get_shard_start(shard)
            if spans > 1 or shard <= s:
                return self.maximum_token()
        else:
            left_part = n >> (64 - self.msb)
            if shard > s:
                left_part += spans - 1
            else:
                left_part += spans - 0
            if left_part >= (1 << self.msb):
                print "greater than 1 << msb", left_part, 1 << self.msb
                return self.maximum_token()
            left_part = (left_part << (64 - msb)) & (2**64-1)
            right_part = self.get_shard_start(shard)
            n = left_part | right_part
        return n - 2**63

    def token_for_prev_shard(self, token, shard, spans = 1):
        n = token + 2**63
        s = self.zero_based_shard_of(n)
        if self.msb == 0:
            n = self.get_shard_start(shard)
            if spans > 1 or shard > s:
                return self.minimum_token()
        else:
            left_part = n >> (64 - msb)
            if shard <= s:
                left_part -= spans - 1
            else:
                left_part -= spans - 0
            if left_part < 0:
                print "less than 0", left_part
                return self.minimum_token()
            left_part = left_part << (64 - msb)
            right_part = self.get_shard_start(shard)
            n = left_part | right_part
        return n - 2**63

    def split_ranges(self, ranges, target_ranges = 2):
        tosplit = []
        while len(ranges) < target_ranges:
            tosplit = ranges
            ranges = []
            for r in tosplit:
                if len(ranges) < target_ranges:
                    start = r[0]
                    end = r[1]
                    mid = (start + end) / 2
                    ranges.append((start, mid))
                    ranges.append((mid, end))
                else:
                    ranges.append(r)
        return ranges

    def add_ranges(self, shard, shard_range_map, ranges):
        if shard in shard_range_map:
            shard_range_map[shard] += ranges
        else:
            shard_range_map[shard] = ranges

    def verify_ranges(self, shard_range_map):
        threads = []
        for shard in shard_range_map:
            t = multiprocessing.Process(target=self.do_verify_ranges, args=(shard_range_map[shard], shard))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()


    def merge(self, intervals):
        if not intervals:
            return []
        data = []
        for interval in intervals:
            data.append((interval[0], 0))
            data.append((interval[1], 1))
        data.sort()

        merged = []
        stack = [data[0]]
        for i in xrange(1, len(data)):
            d = data[i]
            if d[1] == 0:
                stack.append(d)
            elif d[1] == 1:
                if stack:
                    start = stack.pop()
                if len(stack) == 0:
                    merged.append((start[0], d[0]))
        return merged

    def verify_merge_ragnes(self, pr_ranges, npr_ranges, shard_range_map):
        ranges = []
        for shard in shard_range_map:
            for r in shard_range_map[shard]:
                ranges.append(r)
        merged_ranges = self.merge(ranges)
        after = sorted(merged_ranges)
        before = sorted(self.merge(pr_ranges + npr_ranges))
        diff = set(before) - set(after)
        if diff:
            print "ERROR: range merge diff {}", diff

    def do_verify_ranges(self, ranges, shard):
        delta= 2**50
        for r in ranges:
            t1 = r[0]
            t2 = r[1]
            s1 = self.shard_of(t1)
            s2 = self.shard_of(t2)
            for token in xrange(t2, t1, -delta):
                s = self.shard_of(token)
                if s != shard:
                    print 'ERROR: shard={}, range={}, shard({},{}), token={}, shard_for_token={}'.format(shard, r, s1, s2, token, s)

    def split_range_to_shards(self, r):
        shard_range_map = {}
        t1 = r[0]
        t2 = r[1]
        if t1 > t2:
            print "ERROR: wrapping range", r
            sys.exit(-1)
        s1 = self.shard_of(t1)
        s2 = self.shard_of(t2)
        while t1 < t2:
            prev_shard = s2 - 1
            if prev_shard < 0:
                prev_shard = self.nr_shards -1
            token = self.token_for_prev_shard(t2, s2) - 1
            if token > t1:
                new_range = (token, t2)
                self.add_ranges(s2, shard_range_map, [new_range])
            else:
                new_range = (t1, t2)
                self.add_ranges(s2, shard_range_map, [new_range])
            t2 = token
            s2 = prev_shard
        return shard_range_map

    def get_local_range(self, keyspace, primary_range=True, nonprimary_range=True):
        pr_ranges = []
        npr_ranges = []
        url = 'http://{}:10000/storage_service/describe_ring/{}'.format(self.api_host, keyspace)
        text = ''
        try:
            r = requests.get(url=url, timeout=self.timeout)
            text = r.text
            r.raise_for_status()
            data = json.loads(text)
        except Exception as e:
            print "Error: API failed: {}: {}".format(e, text)
            sys.exit(-1)
        for d in data:
            primary_ip = d['endpoints'][0]
            nonprimary_ips = d['endpoints'][1:]
            if primary_range:
                if self.node_ip == primary_ip:
                    st = int(d['start_token'])
                    et = int(d['end_token'])
                    if st > et:
                        r = (self.minimum_token(), et)
                        pr_ranges.append(r)
                        r = (st, self.maximum_token())
                        pr_ranges.append(r)
                    else:
                        r = (st, et)
                        pr_ranges.append(r)
            if nonprimary_range:
                if self.node_ip in nonprimary_ips:
                    st = int(d['start_token'])
                    et = int(d['end_token'])
                    if st > et:
                        r = (minimum_token(), et)
                        npr_ranges.append(r)
                        r = (st, maximum_token())
                        npr_ranges.append(r)
                    else:
                        r = (st, et)
                        npr_ranges.append(r)
        return (pr_ranges, npr_ranges)

def show_succeed_fail_nr(tag, cmds_ok, cmds_fail, nr_repair_jobs):
    print '{}: SUCCEEDED={}, FAILED={}, TOTAL={}'.format(tag, len(cmds_ok), len(cmds_fail), nr_repair_jobs)

def get_db_file_name(keyspace):
    return 'scyllarepair_results_{}.json'.format(keyspace)

idx = 0
def get_idx():
    global idx
    idx += 1
    return idx

REPAIR_STATUS_SUCCESSFUL = 'SUCCESSFUL'
REPAIR_STATUS_FAILED = 'FAILED'

def run_repair_with_shard(api_host, keyspace, primary_range, nonprimary_range, msb, nr_shards, ranges_per_repair, repair_local_dc_only=False, stop_on_failure=False, timeout=10, target_nr_ranges=200):
    db_file = get_db_file_name(keyspace)
    db = None
    try:
        os.remove(db_file)
    except OSError:
        pass
    try:
        db = TinyDB(db_file)
    except Exception as e:
        print "Can not create meta file for repair: {}".format(e)
        return REPAIR_STATUS_FAILED

    print "############  SCYLLA REPAIR: MODE=NORMAL     START #############"
    partitioner = murmur3_partitioner(api_host, msb, nr_shards, timeout)
    pr_ranges, npr_ranges = partitioner.get_local_range(keyspace, primary_range, nonprimary_range)
    node_ip = partitioner.node_ip
    shard_range_map = {}
    for r in pr_ranges + npr_ranges:
        shard_range = partitioner.split_range_to_shards(r)
        for k in shard_range:
            if k in shard_range_map:
                shard_range_map[k] += shard_range[k]
            else:
                shard_range_map[k] = shard_range[k]
    shard_has_no_work = range(nr_shards)
    nr_ranges_on_all_shards = 0
    for k in shard_range_map:
        shard_has_no_work.remove(k)
        ranges = shard_range_map[k]
        nr_ranges = len(ranges)
        if nr_ranges < target_nr_ranges:
            shard_range_map[k] = partitioner.split_ranges(ranges, target_nr_ranges)
        nr = len(shard_range_map[k])
        nr_ranges_on_all_shards += nr
        print "Repair node ip={}, shard={:<3}, ranges={}, ranges_after_split={}".format(node_ip, k, nr_ranges, nr)

    ranges_to_repair_map = {}
    nr_repair_jobs = 0
    for shard in shard_range_map:
        if shard_range_map[shard]:
            ranges = ['{}:{}'.format(r[0], r[1]) for r in shard_range_map[shard]]
            ranges_for_shard = [','.join(ranges[i:i + ranges_per_repair]) for i in xrange(0, len(ranges), ranges_per_repair)]
            ranges_to_repair_map[shard] = [(get_idx(), r) for r in ranges_for_shard]
            db_rows = []
            for idx, ranges in ranges_to_repair_map[shard]:
                db_row = {'status' : 'NOT_STARTED',  "ranges" :  ranges , "idx" : idx, "shard": shard}
                db_rows.append(db_row)
                nr_repair_jobs = nr_repair_jobs + 1
            db.insert_multiple(db_rows)

    print "Repair Summary: node ip={}, msb={}, nr_shards={}, nr_ranges_on_all_shards={}, nr_repair_jobs={}, ranges_per_repair_job={}, primary_ranges={}, non_primary_ranges={}, local={}, shard_has_no_work={}".format(node_ip,
           msb, nr_shards, nr_ranges_on_all_shards, nr_repair_jobs, ranges_per_repair, len(pr_ranges), len(npr_ranges), repair_local_dc_only, shard_has_no_work)

    partitioner.verify_ranges(shard_range_map)
    partitioner.verify_merge_ragnes(pr_ranges, npr_ranges, shard_range_map)

    print "Start to repair ..."
    time.sleep(3)
    local_dc_name = partitioner.local_dc_name
    status = do_repair(db, keyspace, node_ip, ranges_to_repair_map, timeout, nr_repair_jobs, local_dc_name, repair_local_dc_only, stop_on_failure)
    print "############  SCYLLA REPAIR: MODE=NORMAL     END   #############"
    return status

def run_repair_with_shard_cont(api_host, keyspace, repair_local_dc_only=False, stop_on_failure=False, timeout=10):
    print "############  SCYLLA REPAIR: MODE=CONTINUE START #############"
    db = TinyDB(get_db_file_name(keyspace))
    query = Query()
    failed = db.search(query.status != 'SUCCESSFUL')
    ranges_to_repair_map = {}
    nr_repair_jobs = 0
    for f in failed:
        idx = f['idx']
        shard = f['shard']
        ranges = f['ranges']
        ranges_to_repair_map.setdefault(shard,[]).append((idx, ranges))
        nr_repair_jobs = nr_repair_jobs +1
    partitioner = murmur3_partitioner(api_host)
    node_ip = partitioner.node_ip
    local_dc_name = partitioner.local_dc_name
    status = do_repair(db, keyspace, node_ip, ranges_to_repair_map, timeout, nr_repair_jobs, local_dc_name, repair_local_dc_only, stop_on_failure)
    print "############  SCYLLA REPAIR: MODE=CONTINUE END   #############"
    return status

def do_yield():
    time.sleep(0)

def on_failure(shard, kill, stop_on_failure):
    if stop_on_failure:
        print "Stop repair on shard {} because --stop-on-failure is set".format(shard)
        kill.set()

def report_status(status, repair_id, ranges, nr_repair_jobs_done, nr_repair_jobs, node_ip, keyspace, shard, cmds_ok, cmds_fail, start_time):
    nr_repair_jobs_done[0] = nr_repair_jobs_done[0]+ 1;
    percentage = "%.2f" % ((nr_repair_jobs_done[0]) * 100.0 / nr_repair_jobs)
    now = datetime.datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    elapsed = (now - start_time).seconds
    print "[{}] Repair id {:<5} for node {} keyspace {} on shard {:<3}: [{:4}/{:<4}], done={:5}%, status={}, seconds={}, succeeded={}, failed={}".format(ts,
        repair_id, node_ip, keyspace, shard, nr_repair_jobs_done[0] , nr_repair_jobs, percentage, status, elapsed, len(cmds_ok), len(cmds_fail))

def do_repair_for_shard(db, local_dc_name, keyspace, node_ip, shard, idx_ranges, timeout, nr_repair_jobs, nr_repair_jobs_done, lock, kill, cmds_ok, cmds_fail, repair_local_dc_only, stop_on_failure):
    query = Query()
    for idx, ranges in idx_ranges:
        do_yield()
        start_time = datetime.datetime.now()
        url = 'http://{}:10000/storage_service/repair_async/{}?ranges={}'.format(api_host, keyspace, ranges)
        if repair_local_dc_only:
            url = 'http://{}:10000/storage_service/repair_async/{}?ranges={}&dataCenters={}'.format(api_host, keyspace, ranges, local_dc_name)
        text = ''
        try:
            r = requests.post(url, timeout=timeout)
            text = r.text
            r.raise_for_status()
            repair_id = r.text
        except Exception as e:
            with lock:
                cmds_fail.append(r)
                db.update({'status' : 'API_FAILED'}, (query.idx == idx) & (query.shard == shard))
                print "Error: API failed: {}".format(e, text)
                on_failure(shard, kill, stop_on_failure)
            continue

        do_yield()

        url = 'http://{}:10000/storage_service/repair_async/{}'.format(api_host, keyspace)
        payload={'id': repair_id}
        while not kill.is_set() :
            do_yield()
            status = 'API_FAILED'
            text = ''
            try:
                r = requests.get(url, params=payload, timeout=timeout)
                text = r.text
                r.raise_for_status()
                status = r.text.strip('"')
            except Exception as e:
                print "Error: API failed: {}".format(e, text)

            if status == "SUCCESSFUL":
                with lock:
                    cmds_ok.append(r)
                    db.update({'status' : 'SUCCESSFUL'}, (query.idx == idx) & (query.shard == shard))
                    report_status(status, repair_id, ranges, nr_repair_jobs_done, nr_repair_jobs, node_ip, keyspace, shard, cmds_ok, cmds_fail, start_time)
                break
            elif status == "FAILED" or status == "API_FAILED":
                with lock:
                    cmds_fail.append(r)
                    db.update({'status' : status}, (query.idx == idx) & (query.shard == shard))
                    report_status(status, repair_id, ranges, nr_repair_jobs_done, nr_repair_jobs, node_ip, keyspace, shard, cmds_ok, cmds_fail, start_time)
                    on_failure(shard, kill, stop_on_failure)
                break
            if not kill.is_set():
                time.sleep(0.2)

        if kill.is_set():
            print "Stop repair for shard {}".format(shard)
            break

def do_repair(db, keyspace, node_ip, ranges_to_repair_map, timeout, nr_repair_jobs, local_dc_name, repair_local_dc_only=False, stop_on_failure=False):
    nr_repair_jobs_done = [0]
    threads = []
    cmds_ok = []
    cmds_fail = []
    lock = threading.Lock()
    kill = threading.Event()
    for shard, idx_ranges in ranges_to_repair_map.iteritems():
        t = threading.Thread(target=do_repair_for_shard, args=(db, local_dc_name, keyspace, node_ip, shard, idx_ranges, timeout, nr_repair_jobs, nr_repair_jobs_done, lock, kill, cmds_ok, cmds_fail, repair_local_dc_only, stop_on_failure))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    while True in [t.isAlive() for t in threads]:
        try:
            [t.join(1) for t in threads if t is not None and t.isAlive()]
        except KeyboardInterrupt:
            print "Stop repair ..."
            kill.set()

    show_succeed_fail_nr("REPAIR SUMMARY", cmds_ok, cmds_fail, nr_repair_jobs)

    if len(cmds_fail) == 0 and len(cmds_ok) == nr_repair_jobs:
        return REPAIR_STATUS_SUCCESSFUL
    else:
        return REPAIR_STATUS_FAILED

if __name__ == '__main__':
    eg = '''
    Examples:
    1) Run repair for both primary ranges and non-primary ranges
    ./scyllarepair.py --keyspace myks --pr --npr --msb 0 --shards 32
    2) Run with --cont option to rerun repair for the failed range
    ./scyllarepair.py --keyspace myks --cont
    '''
    parser = argparse.ArgumentParser(description='Scylla Repair' + eg , formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--cont', help='Rerun repair for the failed sub subranges', action='store_true')
    parser.add_argument('--keyspace', help='Keyspace to repair', required=True)
    parser.add_argument('--apihost', help='HTTP API HOST', required=False, default='127.0.0.1')
    parser.add_argument('--pr', help='Repair primary ranges', action='store_true')
    parser.add_argument('--npr', help='Repair non primary ranges', action='store_true')
    parser.add_argument('--local', help='Repair only local DC', action='store_true')
    parser.add_argument('--shards', help='Number of cpus scylla uses', required=True, type=int)
    parser.add_argument('--msb', help='murmur3_partitioner_ignore_msb_bits', required=True, type=int)
    parser.add_argument('--ranges-per-repair', help='Number of ranges per repair job', required=False, type=int, default=1)
    parser.add_argument('--stop-on-failure', help='Stop when repair failure happens', action='store_true')
    args = parser.parse_args()
    keyspace = args.keyspace
    primary_range = args.pr
    nonprimary_range = args.npr
    repair_local_dc_only = args.local
    stop_on_failure = args.stop_on_failure
    ranges_per_repair = args.ranges_per_repair
    api_host = args.apihost
    nr_shards = args.shards
    msb = args.msb

    if args.cont:
        status = run_repair_with_shard_cont(api_host, keyspace, repair_local_dc_only, stop_on_failure)
        if status == REPAIR_STATUS_SUCCESSFUL:
            sys.exit(0)
        else:
            sys.exit(-1)

    if primary_range == False and nonprimary_range == False:
        print "Error: Specify --pr (PrimaryRange) and/or --npr (NonPrimaryRange) to repair"
        sys.exit(-1)

    status = run_repair_with_shard(api_host, keyspace, primary_range, nonprimary_range, msb, nr_shards, ranges_per_repair, repair_local_dc_only, stop_on_failure)
    if status == REPAIR_STATUS_SUCCESSFUL:
        sys.exit(0)
    else:
        sys.exit(-1)
