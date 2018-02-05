#!/usr/bin/env python3

import json
import glob
import sys

from os import listdir, path, isdir

for folder in listdir("data/results"):
  if isdir(path.join("data/results", folder)):
    for file in glob.glob(path.join("data/results", folder, "*.json")):
      data = json.load(open(path))

      if "program_run" in data:
        for id, run_data in data["program_run"]:
          run_data['graph'] = run_data['graph'].replace('/home/kit/iti/ji4215', 'data')

      if "clustering" in data:
        for id, clustering_data in data["clustering"]:
          if clustering_data['path'].startswith("clusterings/"):
            clustering_data['path'] = "data/results/{}/{}".format(folder, clustering_data['path'])

      with open(file, 'w') as f:
        json.dump(data, f)
