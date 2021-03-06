#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import sys
import time
import numpy as np
import cloudpickle as cp
import random
import string

import zmq

# from cloudburst.client.client import CloudburstConnection
from anna.client import AnnaTcpClient
from anna.lattices import LWWPairLattice
# from cloudburst.server.benchmarks import (
#     composition,
#     locality,
#     lambda_locality,
#     mobilenet,
#     predserving,
#     scaling,
#     utils,
#     test_anna
# )
import cloudburst.server.utils as sutils

BENCHMARK_START_PORT = 3000

logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')
def str_generator(n):
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(n))

def benchmark(ip, routing_address, tid):
    kvs = AnnaTcpClient(routing_address, ip, local=False, offset=tid + 10)
    
    ctx = zmq.Context(1)

    benchmark_start_socket = ctx.socket(zmq.PULL)
    benchmark_start_socket.bind('tcp://*:' + str(BENCHMARK_START_PORT + tid))
    logging.info(str(BENCHMARK_START_PORT + tid))
    # kvs = cloudburst.kvs_client
    
    while True:
        msg = benchmark_start_socket.recv_string()
        logging.info('Receive message: %s' % (msg))
        # splits = msg.split(':')

        # resp_addr = splits[0]
        num_requests = int(msg)

        # sckt = ctx.socket(zmq.PUSH)
        # sckt.connect('tcp://' + resp_addr + ':3000')
        sample = np.random.zipf(2, num_requests)
        total_time = []
        for i in range(num_requests):
            key = str(sample[i]).zfill(8)
            # key = str(sample[i])
            arr = str_generator(256000)
            lattice = LWWPairLattice(0, arr.encode())
            start = time.time()
            put_res = kvs.put(key, lattice)
            get_res = kvs.get(key)
            end = time.time()
            if put_res[key] != 0:
                logging.info('PUT Error: %d' % (put_res[key]))
                logging.info('GET Error: %s' % (get_res))
            total_time += [end - start]
        total = sum(total_time)
        thruput = num_requests * 2 / total
        logging.info(' Throughput(ops/sec): %.2f' % (thruput))
        # new_total = cp.dumps(total_time)
        # sckt.send(new_total);
        # logging.info("Finsh sending requests")


# def run_bench(bname, num_requests, cloudburst, kvs, sckt, create=False):
#     logging.info('Running benchmark %s, %d requests.' % (bname, num_requests))

#     if bname == 'composition':
#         total, scheduler, kvs, retries = composition.run(cloudburst, num_requests,
#                                                          sckt)
#     elif bname == 'locality':
#         total, scheduler, kvs, retries = locality.run(cloudburst, num_requests,
#                                                       create, sckt)
#     elif bname == 'redis' or bname == 's3':
#         total, scheduler, kvs, retries = lambda_locality.run(bname, kvs,
#                                                              num_requests,
#                                                              sckt)
#     elif bname == 'predserving':
#         total, scheduler, kvs, retries = predserving.run(cloudburst, num_requests,
#                                                          sckt)
#     elif bname == 'mobilenet':
#         total, scheduler, kvs, retries = mobilenet.run(cloudburst, num_requests,
#                                                        sckt)
#     elif bname == 'scaling':
#         total, scheduler, kvs, retries = scaling.run(cloudburst, num_requests,
#                                                      sckt, create)
#     elif bname == 'anna':
#         total, scheduler, kvs, retries = test_anna.run(bname, num_requests,sckt)
#     else:
#         logging.info('Unknown benchmark type: %s!' % (bname))
#         sckt.send(b'END')
#         return

#     # some benchmark modes return no results
#     if not total:
#         sckt.send(b'END')
#         logging.info('*** Benchmark %s finished. It returned no results. ***'
#                      % (bname))
#         return
#     else:
#         sckt.send(b'END')
#         logging.info('*** Benchmark %s finished. ***' % (bname))

#     logging.info('Total computation time: %.4f' % (sum(total)))
#     if len(total) > 0:
#         utils.print_latency_stats(total, 'E2E', True)
#     if len(scheduler) > 0:
#         utils.print_latency_stats(scheduler, 'SCHEDULER', True)
#     if len(kvs) > 0:
#         utils.print_latency_stats(kvs, 'KVS', True)
#     logging.info('Number of KVS get retries: %d' % (retries))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    else:
        conf_file = 'conf/cloudburst-config.yml'

    conf = sutils.load_conf(conf_file)
    bench_conf = conf['benchmark']

    benchmark(conf['ip'], bench_conf['cloudburst_address'],
              int(bench_conf['thread_id']))
