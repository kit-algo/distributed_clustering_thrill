#!/usr/bin/env python3

import json
import glob
import sys

from os import listdir, path

for folder in listdir("data/results"):
  if path.isdir(path.join("data/results", folder)):
    for file in glob.glob(path.join("data/results", folder, "*.json")):
      print(file)
      data = json.load(open(file))

      if "program_run" in data:
        for index, run_data in data["program_run"].items():
          if run_data['graph'].startswith('/home/kit/iti/ji4215'):
            old = run_data['graph']
            run_data['graph'] = run_data['graph'].replace('/home/kit/iti/ji4215', 'data')
            print("fixed graph path from {} to {}".format(old, run_data['graph']))

      if "clustering" in data:
        for index, clustering_data in data["clustering"].items():
          if 'path' in clustering_data:
            if clustering_data['path'].startswith("clusterings/"):
              old = clustering_data['path']
              clustering_data['path'] = "data/results/{}/{}".format(folder, clustering_data['path'])
              print("fixed clustering path from {} to {}".format(old, clustering_data['path']))
            if '@@@@-#####' in clustering_data['path']:
              old = clustering_data['path']
              clustering_data['path'] = clustering_data['path'].replace('@@@@-#####', '*')
              print("fixed clustering path from {} to {}".format(old, clustering_data['path']))

      with open(file, 'w') as f:
        json.dump(data, f)