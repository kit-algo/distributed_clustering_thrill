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

graphs = [
  # '/home/kit/iti/kp0036/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin',
  '/home/kit/iti/kp0036/graphs/com-friendster-preprocessed-*.bin',
  '/home/kit/iti/kp0036/graphs/uk-2002.metis-preprocessed-*.bin'
]

graphs.each do |graph|
  bins.each do |bin|
    1.times do
      puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/dlm -v EXECUTABLE=/home/kit/iti/kp0036/code/prototypes/thrill_louvain/release/#{bin} /home/kit/iti/kp0036/code/scripts/moab/pre_exp_dlm.sh`
    end
  end
end
