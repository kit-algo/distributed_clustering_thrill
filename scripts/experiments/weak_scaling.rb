#!/usr/bin/env ruby

jobs = [
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_4000000-sorted-preprocessed-*.bin",   00, 10, 1],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_8000000-sorted-preprocessed-*.bin",   00, 10, 2],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin",  00, 20, 4],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_32000000-sorted-preprocessed-*.bin",  00, 30, 8],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_64000000-sorted-preprocessed-*.bin",  01, 00, 16],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_128000000-sorted-preprocessed-*.bin", 01, 00, 32],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_256000000-sorted-preprocessed-*.bin", 02, 00, 64],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_512000000-sorted-preprocessed-*.bin", 05, 00, 128],
]

jobs.each do |job|
  graph, hours, minutes, nodes = *job
  puts `msub -v GRAPH=#{graph} -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm.sh`
  puts `msub -v GRAPH=#{graph} -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_with_seq.sh`
  puts `msub -v GRAPH=#{graph} -l walltime=#{"%02d" % (hours * 2)}:#{"%02d" % (minutes * 2)}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_map_eq.sh`
end
