#!/usr/bin/env python3

import sys
import re
import multiprocessing
from os import listdir, path
from subprocess import check_output

# convert clusterings
def convert(file):
  input = path.join(sys.argv[1], "clusterings", file)
  output = path.join(sys.argv[1], "clusterings", "{}.bin".format(file))
  print(["./convert_infomap_clustering_to_binary", input, output])
  print(check_output(["./convert_infomap_clustering_to_binary", input, output]))

file_pattern = re.compile('\S+_\d+.\d+_of_\d+$')
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count)
pool.map(convert, [file for file in listdir(path.join(sys.argv[1], "clusterings")) if file_pattern.match(file)])
