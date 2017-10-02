#!/usr/bin/env ruby

jobs = [
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_4000000-sorted-preprocessed-*.bin", 0, 10, 1],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_4000000-sorted-preprocessed-*.bin", 0, 10, 2],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_4000000-sorted-preprocessed-*.bin", 0, 10, 4],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_4000000-sorted-preprocessed-*.bin", 0, 10, 8],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_4000000-sorted-preprocessed-*.bin", 0, 10, 16],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_4000000-sorted-preprocessed-*.bin", 0, 10, 32],
]

`mkdir clusterings`

jobs.each do |job|
  graph, hours, minutes, nodes = *job
  puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/mod -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm.sh`
  puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/mods -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_with_seq.sh`
  puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/me -l walltime=#{"%02d" % (hours * 2)}:#{"%02d" % (minutes * 2)}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_map_eq.sh`
end
