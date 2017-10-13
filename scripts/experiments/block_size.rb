#!/usr/bin/env ruby

jobs = [
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin",  00, 10, 4, 1024 * 4],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin",  00, 10, 4, 1024 * 16],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin",  00, 10, 4, 1024 * 64],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin",  00, 10, 4, 1024 * 256],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin",  00, 10, 4, 1024 * 1024],
]

jobs.each do |job|
  graph, hours, minutes, nodes, block_size = *job
  puts `msub -v GRAPH=#{graph} -v THRILL_BLOCK_SIZE=#{block_size} -l walltime=#{"%02d" % (hours * 2)}:#{"%02d" % (minutes * 2)}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_map_eq.sh`
end
