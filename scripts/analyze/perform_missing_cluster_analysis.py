#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import json
import glob
import sys
import re
import multiprocessing

data = {}

for path in glob.glob(sys.argv[1]):
  for typename, items in json.load(open(path)).items():
    if typename in data:
      data[typename].update(items)
    else:
      data[typename] = items

frames = { typename: pd.DataFrame.from_dict(items, orient='index') for typename, items in data.items() }

from os import listdir, path
from subprocess import check_output

file_pattern = re.compile('^(.+-(\d+))-\d+-\d+\.bin$')
jobid_pattern = re.compile('^clusterings/.+-(\d+)-@@@@-#####.bin$')

base_dir = path.dirname(sys.argv[1])

def work(clustering_path):
  if not (frames['clustering']['path'] == clustering_path).any():
    jobid = int(jobid_pattern.match(clustering_path).group(1))
    df = frames['algorithm_run'].merge(frames['program_run'].loc[frames['program_run']["job_id"] == jobid], left_on='program_run_id', right_index=True)
    job_data = df.iloc[0]
    index = df.index[0]
    graph_files = job_data['graph'].replace('/home/kit/iti/ji4215', '/algoDaten/zeitz')
    clustering_files = path.join(base_dir, clustering_path.replace('@@@@-#####', '*'))

    print(["./streaming_clustering_analyser", graph_files, clustering_files, job_data['node_count']])
    output = check_output(["./streaming_clustering_analyser", graph_files, clustering_files, str(job_data['node_count'])]).decode("utf-8")

    tmpfile_name = "_tmp_{}".format(jobid)
    tmpfile = open(tmpfile_name, "w")
    tmpfile.write(output)
    tmpfile.write("#LOG# clustering/0/path: {}\n".format(clustering_path))
    tmpfile.write("#LOG# clustering/0/source: computation\n")
    tmpfile.write("#LOG# clustering/0/algorithm_run_id: {}\n".format(index))
    tmpfile.close()
    check_output(["./report_to_json.rb", tmpfile_name])
    check_output(["rm", tmpfile_name])

# clusterings/me-14169048-@@@@-#####.bin
count = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=count)
pool.map(work, set(["clusterings/{}-@@@@-#####.bin".format(file_pattern.match(file).group(1)) for file in listdir(path.join(base_dir, "clusterings")) if file_pattern.match(file)]))

def work2(row):
  index, row_data = row

  graph_files = row_data['graph'].replace('/home/kit/iti/ji4215', '/algoDaten/zeitz')
  clustering_files = path.join(base_dir, row_data['path'].replace('@@@@-#####', '*'))

  print(["./streaming_clustering_analyser", graph_files, clustering_files, row_data['node_count']])
  output = check_output(["./streaming_clustering_analyser", graph_files, clustering_files, str(row_data['node_count'])]).decode("utf-8")
  output.replace("clustering/0", "clustering/{}".format(index))

  tmpfile_name = "_tmp_{}".format(index)
  tmpfile = open(tmpfile_name, "w")
  tmpfile.write(output)
  tmpfile.close()
  check_output(["./report_to_json.rb", tmpfile_name])
  check_output(["rm", tmpfile_name])

if (not 'modularity' in frames['clustering']) or (not 'map_equation' in frames['clustering']) or (not 'cluster_count' in frames['clustering']):
  clusterings = frames['clustering']
else:
  clusterings = frames['clustering'][frames['clustering']['modularity'].isnull() | frames['clustering']['map_equation'].isnull() | frames['clustering']['cluster_count'].isnull()]

df = clusterings \
  .merge(frames['algorithm_run'], left_on='algorithm_run_id', right_index=True) \
  .merge(frames['program_run'], left_on='program_run_id', right_index=True)
pool.map(work2, df.iterrows())
