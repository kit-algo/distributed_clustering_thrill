#!/usr/bin/env python3

import json
import glob
import sys

bin_to_algo = {
  "/home/kit/iti/ji4215/code/release/dlslm": "synchronous local moving with modularity",
  "/home/kit/iti/ji4215/code/release/dlslm_no_contraction": "synchronous local moving with modularity",
  "/home/kit/iti/ji4215/code/release/dlslm_map_eq": "synchronous local moving with map equation"
}

for path in glob.glob(sys.argv[1]):
  data = json.load(open(path))
  if len(data['program_run'].keys()) != 1:
    print("not exactly one program_run keys")
    raise
  program_run_key = next(iter(data['program_run'].keys()))

  if len(data['algorithm_run'].keys()) != 1:
    print("not exactly one program_run keys")
    raise
  algorithm_run_key = next(iter(data['algorithm_run'].keys()))

  if not 'program_run_id' in data['algorithm_run'][algorithm_run_key]:
    data['algorithm_run'][algorithm_run_key]['program_run_id'] = program_run_key
    data['algorithm_run'][algorithm_run_key]['algorithm'] = bin_to_algo[data['program_run'][program_run_key]['binary']]


    with open(path, 'w') as f:
      json.dump(data, f)
