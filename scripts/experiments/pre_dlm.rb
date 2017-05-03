#!/usr/bin/env ruby

bins = [
  'dlm_cluster_1',
  'dlm_cluster_2',
  'dlm_cluster_4',
  'dlm_cluster_8',
  'dlm_cluster_dyn',
  'dlm_moved_1',
  'dlm_moved_2',
  'dlm_moved_4',
  'dlm_moved_8',
  'dlm_moved_dyn'
]

bins.each do |bin|
  3.times do
    `msub -v GRAPH=graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin -v CLUSTERING=clusterings/pre/dlm -v EXECUTABLE=/home/kit/iti/kp0036/code/prototypes/thrill_louvain/release/#{bin} ~/code/scripts/moab/pre_exp_dlm.sh`
  end
end
