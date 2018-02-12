#!/usr/bin/env python3

# Time for partitioning : 249.248 (sec)

import json
import glob
import sys
import re
import os

time_pattern = re.compile('^Time for partitioning : (\d+.\d+) \(sec\)$')

for path in glob.glob(sys.argv[1]):
  data = json.load(open(path))

  if 'program_run' in data:
    if len(data['program_run'].keys()) != 1:
      print("not exactly one program_run keys")
      raise
    program_run_key = next(iter(data['program_run'].keys()))

    if len(data['algorithm_run'].keys()) != 1:
      print("not exactly one program_run keys")
      raise
    algorithm_run_key = next(iter(data['algorithm_run'].keys()))

    if data['algorithm_run'][algorithm_run_key]['algorithm'] == 'relax map':
      with open("{}/job_uc1_{}.out".format(os.path.dirname(path), data['program_run'][program_run_key]['job_id'])) as f:
        for line in f:
          m = time_pattern.match(line)
          if m:
            print("fixing relax runtime from {} to {}".format(data['algorithm_run'][algorithm_run_key]['runtime'], m.group(1)))
            data['algorithm_run'][algorithm_run_key]['runtime'] = float(m.group(1))

      with open(path, 'w') as f:
        json.dump(data, f)

