import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import json
import glob

data = {}

for path in glob.glob(sys.argv[1]):
  for typename, items in json.load(open(path)).items():
    if typename in data:
      for key, object_data in items.items():
        if key in data[typename]:
          data[typename][key].update(object_data)
        else:
          data[typename][key] = object_data
    else:
      data[typename] = items

frames = { typename: pd.DataFrame.from_dict(items, orient='index') for typename, items in data.items() }

distr = frames['cluster_size_distribution'][:1].drop('algorithm_level_id', axis=1).transpose().dropna()
distr.index = pd.to_numeric(distr.index)
distr.reindex(range(1, distr.index.max() + 1)).fillna(0).plot(kind='bar')
plt.hist([toplot.index, toplot.index], weights=[toplot['0068c903-3b38-48e8-b590-5827455f8914'], toplot['0051fd0e-3892-4261-9849-8d32bbe8a950']], bins=toplot.index, label=['foo', 'bar'])
plt.legend()


pd.merge(frames['cluster_size_distribution'], frames['algorithm_level'], left_on='algorithm_level_id', right_index=True)
  .groupby('algorithm_run_id')
  .apply(lambda x: x.sort('node_count', ascending=False)[:1])
  .merge(frames['algorithm_run'], left_on='algorithm_run_id', right_index=True)
  .merge(frames['program_run'], left_on='program_run_id', right_index=True)



amazon_runs = frames['program_run'][frames['program_run'].graph.str.contains('amazon')]
runs_to_compare = frames['algorithm_run'][(frames['algorithm_run'].order == 'original') & ((frames['algorithm_run'].partition_count == 32) | (frames['algorithm_run'].partition_count.isnull()))]
toplot = pd.merge(frames['cluster_size_distribution'], frames['algorithm_level'], left_on='algorithm_level_id', right_index=True) \
  .groupby('algorithm_run_id') \
  .apply(lambda x: x.sort('node_count', ascending=False)[:1]) \
  .merge(runs_to_compare, left_on='algorithm_run_id', right_index=True) \
  .merge(amazon_runs, left_on='program_run_id', right_index=True)
toplot = toplot[:3]
toplot = toplot.fillna({'partition_algorithm': 'sequential'})
toplot = toplot.set_index('partition_algorithm')
toplot = toplot[frames['cluster_size_distribution'].keys()].drop('algorithm_level_id', axis=1).transpose().dropna()
toplot.index = pd.to_numeric(toplot.index)
toplot = toplot.reindex(range(1, toplot.index.max() + 1)).fillna(0)
toplot.plot.bar()
plt.hist([toplot.index for column in toplot.columns], weights=[toplot[column] for column in toplot.columns], bins=toplot.index, label=list(toplot.columns))
plt.legend()



frames['algorithm_level'] \
  .merge(runs_to_compare, left_on='algorithm_run_id', right_index=True) \
  .merge(amazon_runs.drop('node_count', axis=1), left_on='program_run_id', right_index=True) \
  .groupby('algorithm_run_id') \
  .apply(lambda x: x.sort('node_count', ascending=False))[['node_count', 'cluster_count', 'partition_algorithm']]


