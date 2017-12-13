#!/usr/bin/env ruby

jobs = [
  ["#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin",  00, 20, 1],
  ["#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_32000000-sorted-preprocessed-*.bin",  00, 29, 2],
  ["#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_64000000-sorted-preprocessed-*.bin",  01, 00, 4],
  ["#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_128000000-sorted-preprocessed-*.bin", 01, 00, 8],
  ["#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_256000000-sorted-preprocessed-*.bin", 01, 29, 16],
  ["#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_512000000-sorted-preprocessed-*.bin", 02, 00, 32],
]

`mkdir clusterings`

BLOCK_SIZE = 1024 * 128

jobs.each do |job|
  graph, hours, minutes, nodes = *job
  puts `msub -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/mod -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm.sh`
  puts `msub -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/modnc -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_no_contraction.sh`
  puts `msub -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/me -l walltime=#{"%02d" % (hours * 2)}:#{"%02d" % (minutes * 2)}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_map_eq.sh`
end
