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

import sys
import time
import uuid

from anna.lattices import LWWPairLattice
from anna.client import AnnaTcpClient
import cloudpickle as cp
import numpy as np

from cloudburst.shared.reference import CloudburstReference


def run(cloudburst_client, num_requests, sckt):
   
    # Create all the input data
    sample = np.random.zipf(2, num_requests)
    for i in range(num_requests):
        key = sample[i]
        val = "a";
        arr = val.ljust(1024, 'a')
        start = time.time()
        cloudburst_client.put_object(key, arr)
        cloudburst_client.get_object(key)
        end = time.time()
        total_time += [end - start]
    if sckt:
        sckt.send(cp.dumps(total_time))

    return total_time, [], [], 0
