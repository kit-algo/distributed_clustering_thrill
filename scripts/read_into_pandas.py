import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import json
import glob

data = {}

for path in glob.glob("/foobar/*.json"):
  # file = open('/home/eagle/dev/ma_thesis_code/prototypes/louvain/build/report_0_2017-01-10_at_9f3d1fe4bd3cb7de470d98144831ba0f80d1ecf5.json')
  for typename, items in json.load(open(path)):
    if typename in data:
      data[typename].update(items)
    else:
      data[typename] = items

frames = { typename: pd.DataFrame.from_dict(items, orient='index') for typename, items in data.items() }
