#!/usr/bin/env ruby

require 'securerandom'

jobs = [
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_1000000-sorted-preprocessed-*.bin", "results/weak_scaling_quality/clusterings/mod-13209149-*.bin", 0, 10, 2],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_512000000-sorted-preprocessed-*.bin", "clusterings/me-13191014-*.bin", 2, 0, 32],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_512000000-sorted-preprocessed-*.bin", "clusterings/mod-13191012-*.bin", 2, 0, 32],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_256000000-sorted-preprocessed-*.bin", "clusterings/mod-13191009-*.bin", 2, 0, 16],
]

jobs.each do |job|
  graph, clustering, hours, minutes, nodes = *job
  puts `msub -v GRAPH=#{graph} -v CLUSTERING=#{clustering} -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{nodes}:ppn=28 #{ENV['HOME']}/code/scripts/moab/distributed_clustering_analyser.sh`
end
