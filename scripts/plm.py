#!/usr/bin/env python3

import sys
import os

from networkit import graphio, community, setNumberOfThreads, stopwatch
from glob import glob

print("#LOG# program_run/0/binary: {}".format(sys.argv[0]))
print("#LOG# program_run/0/hosts: 1")
print("#LOG# program_run/0/total_workers: {}".format(16))
print("#LOG# program_run/0/workers_per_host: {}".format(16))
print("#LOG# program_run/0/graph: {}".format(sys.argv[1]))

g = graphio.ThrillGraphBinaryReader().read(sorted(glob(sys.argv[1])))

print("#LOG# program_run/0/node_count: {}".format(g.numberOfNodes()))
print("#LOG# program_run/0/edge_count: {}".format(g.numberOfEdges()))

if 'MOAB_JOBID' in os.environ:
  print("#LOG# program_run/0/job_id: {}".format(os.environ['MOAB_JOBID']))

print("#LOG# algorithm_run/1/program_run_id: 0")
print("#LOG# algorithm_run/1/algorithm: PLM")

setNumberOfThreads(16)
t = stopwatch.Timer()
c = community.detectCommunities(g, inspect=False)
t.stop()

print("#LOG# algorithm_run/1/runtime: {}".format(t.elapsed))

community.writeCommunities(c, sys.argv[2])

print("#LOG# clustering/2/algorithm_run_id: 1")
print("#LOG# clustering/2/source: computation")
print("#LOG# clustering/2/path: {}".format(sys.argv[2]))
