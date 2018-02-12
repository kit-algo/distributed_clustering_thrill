#!/usr/bin/env python3

import sys
import multiprocessing

from os import listdir, path
from subprocess import check_output

def work(mu):
  graph_path = "data/graphs/lfr_params/graph_g2_b1_m0.{}-preprocessed-*.bin".format(mu)
  ground_truth_path = "data/graphs/lfr_params/part_graph_g2_b1_m0.{}-preprocessed-*.bin".format(mu)

  output = check_output(["./streaming_clustering_analyser", graph_path, ground_truth_path, str(1000000)]).decode("utf-8")

  tmpfile_name = "_tmp_{}".format(mu)
  tmpfile = open(tmpfile_name, "w")
  tmpfile.write(output)
  tmpfile.write("#LOG# clustering/0/path: {}\n".format(ground_truth_path))
  tmpfile.write("#LOG# clustering/0/graph: {}\n".format(graph_path))
  tmpfile.write("#LOG# clustering/0/source: ground_truth\n")
  tmpfile.close()
  check_output(["./report_to_json.rb", tmpfile_name])
  check_output(["rm", tmpfile_name])

count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count)
pool.map(work, range(1, 10))
