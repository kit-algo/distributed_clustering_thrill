
# coding: utf-8

# In[1]:


import numpy as np
import matplotlib as mpl
# %matplotlib inline

import pandas as pd

import json
import glob
import os
import os.path

from networkit import *


# In[2]:


def plot_and_save(df, name, kind='line', xlabel=None, ylabel=None, **kwargs):
    ax = df.plot(kind=kind, **kwargs)
    
    if xlabel != None:
        ax.set_xlabel(xlabel)
    if ylabel != None:
        ax.set_ylabel(ylabel)
    
    ax.legend().set_visible(False)
    mpl.pyplot.savefig("../../dist-thrill-cluster/plots/" + name + "_no_legend.png", dpi=300)
    ax.legend().set_visible(True)
    mpl.pyplot.savefig("../../dist-thrill-cluster/plots/" + name + ".png", dpi=300)
    
    df.to_csv("../../dist-thrill-cluster/plots/" + name + ".csv")


# In[3]:


data = {}

for path in glob.glob(os.path.expanduser("/amd.home/home/i11/zeitz/ma/data/results/paper/all_real2/*.json")) + glob.glob(os.path.expanduser("/amd.home/home/i11/zeitz/ma/data/results/paper/all_real_seq/*.json")):
  for typename, items in json.load(open(path)).items():
    if typename in data:
      data[typename].update(items)
    else:
      data[typename] = items

frames = { typename: pd.DataFrame.from_dict(items, orient='index') for typename, items in data.items() }


# In[4]:


dlslm_label = 'DSLM-Mod'
dlslm_me_label = 'DSLM-Map'
seq_postfix = ' w. Seq.'
no_contraction_postfix = ' w/o Contraction'
dlslm_ws_label = dlslm_label + seq_postfix
dlslm_nc_label = dlslm_label + no_contraction_postfix
seq_louvain_label = 'Seq. Louvain'
seq_infomap_label = 'Seq. Infomap'

algo_name_mapping = {
    'synchronous local moving with map equation': dlslm_me_label,
    'synchronous local moving with modularity': dlslm_label,
    'sequential louvain': seq_louvain_label,
    'sequential infomap': seq_infomap_label
}

frames['algorithm_run'].replace({ 'algorithm': algo_name_mapping }, inplace=True)

frames['algorithm_run']['algorithm'] += frames['algorithm_run'].merge(frames['program_run'], left_on='program_run_id', right_index=True, how='left')['switch_to_seq'].map({ False: '', True: seq_postfix, np.NaN: '' })
frames['algorithm_run']['algorithm'] += frames['algorithm_run'].merge(frames['program_run'], left_on='program_run_id', right_index=True, how='left')['contraction'].map({ False: no_contraction_postfix, True: '', np.NaN: '' })


# In[5]:


frames['program_run']['graph_path'] = frames['program_run']['graph']

graph_names = { 
    '/home/kit/iti/kp0036/graphs/uk-2002.metis-preprocessed-*.bin': 'uk-2002', 
    '/home/kit/iti/kp0036/graphs/uk-2007-05.metis-preprocessed-*.bin': 'uk-2007-05', 
    '/home/kit/iti/kp0036/graphs/in-2004.metis-preprocessed-*.bin': 'in-2004', 
    '/home/kit/iti/kp0036/graphs/com-friendster-preprocessed-*.bin': 'com-friendster', 
    '/home/kit/iti/kp0036/graphs/com-lj.ungraph-preprocessed-*.bin': 'com-lj', 
    '/home/kit/iti/kp0036/graphs/com-orkut.ungraph-preprocessed-*.bin': 'com-orkut', 
    '/home/kit/iti/kp0036/graphs/com-youtube.ungraph-preprocessed-*.bin': 'com-youtube', 
    '/home/kit/iti/kp0036/graphs/com-amazon.ungraph-preprocessed-*.bin': 'com-amazon',
    '/home/kit/iti/kp0036/graphs/europe.osm-preprocessed-*.bin': 'osm-europe',
}

frames['program_run'].replace({ 'graph': graph_names }, inplace=True)


# In[7]:


algo_to_baseline = {
  dlslm_label: 'seq_louvain',
  dlslm_ws_label: 'seq_louvain',
  dlslm_nc_label: 'seq_louvain',
  seq_louvain_label: 'seq_louvain',
  seq_infomap_label: 'infomap',
  dlslm_me_label: 'infomap',
}

algo_to_dir = {
  dlslm_label: 'all_real2',
  dlslm_ws_label: 'all_real2',
  dlslm_nc_label: 'all_real2',
  dlslm_me_label: 'all_real2',
  seq_louvain_label: 'all_real_seq',
  seq_infomap_label: 'all_real_seq',
}

graph_to_baseline = { 
    'uk-2002': 'uk-2002.metis-preprocessed.part',
    'uk-2007-05': 'uk-2007-05.metis-preprocessed.part',
    'in-2004': 'in-2004.metis-preprocessed.part',
    'com-friendster': 'com-friendster-preprocessed.part',
    'com-lj': 'com-lj.ungraph-preprocessed.part',
    'com-orkut': 'com-orkut.ungraph-preprocessed.part',
    'com-youtube': 'com-youtube.ungraph-preprocessed.part',
    'com-amazon': 'com-amazon.ungraph-preprocessed.part',
    'osm-europe': 'europe.osm-preprocessed.part',
}


# In[ ]:


def siml(x):
    gt_file = "/amd.home/home/i11/zeitz/ma/data/results/paper/all_real_baseline/clusterings/{}-{}".format(algo_to_baseline[x['algorithm']], graph_to_baseline[x['graph']])
    if not os.path.isfile(gt_file):
        return pd.Series([np.NaN, np.NaN])
    ground_truth = community.readCommunities(gt_file)
    
    if algo_to_dir[x['algorithm']] == 'all_real2':
        files = sorted(glob.glob(os.path.expanduser('/amd.home/home/i11/zeitz/ma/data/results/paper/all_real2/' + x['path'].replace('@@@@-#####', '*'))))
        clustering = community.BinaryEdgeListPartitionReader(0, 4).read(files)
    else:
        clustering = community.readCommunities("/amd.home/home/i11/zeitz/ma/data/results/paper/all_real_seq/" + x['path'])
    
        
    g = graph.Graph(ground_truth.numberOfElements())
    nmi = 1.0 - community.NMIDistance().getDissimilarity(g, clustering, ground_truth)
    ari = 1.0 - community.AdjustedRandMeasure().getDissimilarity(g, clustering, ground_truth)
    return pd.Series([nmi, ari])
    
frames['clustering'][['NMI', 'ARI']] = frames['clustering']     .merge(frames['algorithm_run'], left_on='algorithm_run_id', right_index=True)     .merge(frames['program_run'], left_on='program_run_id', right_index=True)     .apply(siml, axis=1)


# In[ ]:


df = frames['clustering']     .merge(frames['algorithm_run'], left_on='algorithm_run_id', right_index=True)     .merge(frames['program_run'], left_on='program_run_id', right_index=True)     .groupby(['graph', 'algorithm'])['NMI'].mean().to_frame().unstack()     ["NMI"][[dlslm_label, dlslm_nc_label, dlslm_me_label, seq_louvain_label, seq_infomap_label]]
    
df = df.loc[frames['program_run'].sort_values('edge_count')['graph'].dropna().unique()]

plot_and_save(df, "real_world_similarity_NMI", ylabel="NMI", kind='bar')


# In[ ]:


df = frames['clustering']     .merge(frames['algorithm_run'], left_on='algorithm_run_id', right_index=True)     .merge(frames['program_run'], left_on='program_run_id', right_index=True)     .groupby(['graph', 'algorithm'])['ARI'].mean().to_frame().unstack()     ["ARI"][[dlslm_label, dlslm_nc_label, dlslm_me_label, seq_louvain_label, seq_infomap_label]]
    
df = df.loc[frames['program_run'].sort_values('edge_count')['graph'].dropna().unique()]

plot_and_save(df, "real_world_similarity_ARI", ylabel="ARI", kind='bar')

