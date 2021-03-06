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
import zmq

import cloudpickle as cp

from cloudburst.server.benchmarks import utils

logging.basicConfig(filename='log_trigger.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

NUM_THREADS = 8
BENCH_PORT = 3000

ips = []
with open('bench_ips.txt', 'r') as f:
    line = f.readline()
    while line:
        ips.append(line.strip())
        line = f.readline()

msg = sys.argv[1]
# splits = msg.split(':')
num_requests = int(msg)
ctx = zmq.Context()

recv_socket = ctx.socket(zmq.PULL)
recv_socket.bind('tcp://*:' + str(BENCH_PORT))

sent_msgs = 0

if 'create' in msg:
    sckt = ctx.socket(zmq.PUSH)
    sckt.connect('tcp://' + ips[0] + ':' + BENCH_PORT)

    sckt.send_string(msg)
    sent_msgs += 1
else:
    print("start")
    for ip in ips:
        for tid in range(NUM_THREADS):
            sckt = ctx.socket(zmq.PUSH)
            print(str(BENCH_PORT + tid))
            print('tcp://' + ip + ':' + str(BENCH_PORT + tid))
            sckt.connect('tcp://' + ip + ':' + str(BENCH_PORT + tid))
            # sckt.bind('tcp://*:' + str(3005 + tid))
            # receive = ctx.socket(zmq.PULL)
            # receive.connect('tcp://localhost:' + str(3005 + tid))
            # sender = ctx.socket(zmq.PUSH)
            # sender.connect('tcp://' + ip + ':' + str(3005 + tid))
            # sender.send(msg)
            sckt.send_string(msg)
            sent_msgs += 1
        print(ip)
    print(sent_msgs)


epoch_total = []
total = []
end_recv = 0

epoch_recv = 0
epoch = 1
epoch_thruput = 0
epoch_start = time.time()

while end_recv < sent_msgs:
    msg = recv_socket.recv()
    if b'END' in msg:
        end_recv += 1
    else:
        msg = cp.loads(msg)
        if type(msg) == tuple:
            epoch_thruput += msg[0]
            new_tot = msg[1]
        else:
            new_tot = msg

        epoch_total += new_tot
        total += new_tot
        epoch_recv += 1
        print(sent_msgs)
        if epoch_recv == sent_msgs:
            epoch_end = time.time()
            elapsed = epoch_end - epoch_start
            size = len(epoch_total) * 2
            thruput = size / sum(epoch_total)

            logging.info('\n\n*** EPOCH %d ***' % (epoch))
            logging.info(' Operation counts:%d\n' % size)
            logging.info(' Throughput(ops/sec): %.2f' % (thruput))
            # utils.print_latency_stats(epoch_total, 'E2E', True, sum(epoch_total))
            print("Finish")
            epoch_recv = 0
            epoch_thruput = 0
            epoch_total.clear()
            epoch_start = time.time()
            epoch += 1

logging.info('*** END ***')

if len(total) > 0:
    utils.print_latency_stats(total, 'E2E', True)
