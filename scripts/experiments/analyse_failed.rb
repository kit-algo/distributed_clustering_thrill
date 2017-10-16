#!/usr/bin/env ruby

jobs = [
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_512000000-sorted-preprocessed-*.bin", "clusterings/me-13191014-*.bin", 1, 0, 32],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_512000000-sorted-preprocessed-*.bin", "clusterings/mod-13191012-*.bin", 1, 0, 32],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_256000000-sorted-preprocessed-*.bin", "clusterings/mod-13191009-*.bin", 1, 0, 16],
]

jobs.each do |job|
  graph, clustering, hours, minutes, nodes = *job
  puts `msub -v GRAPH=#{graph} -v CLUSTERING=#{clustering} -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{nodes}:ppn=28 #{ENV['HOME']}/code/scripts/moab/distributed_clustering_analyser.sh`
end
