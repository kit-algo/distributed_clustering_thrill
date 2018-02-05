#!/usr/bin/env python3

import sys
import multiprocessing

from os import listdir, path
from subprocess import check_output

def work(exp):
  node_count = 2 ** exp * 1000000
  graph_path = "data/graphs/mu-04/graph_50_10000_mu_0.4_{}-sorted-preprocessed-*.bin".format(node_count)
  ground_truth_path = "data/graphs/mu-04/part_50_10000_mu_0.4_{}-sorted-preprocessed-*.bin".format(node_count)

  output = check_output(["./streaming_clustering_analyser", graph_path, ground_truth_path, str(node_count)]).decode("utf-8")

  tmpfile_name = "_tmp_".format(node_count)
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
pool.map(work, range(0, 10))
