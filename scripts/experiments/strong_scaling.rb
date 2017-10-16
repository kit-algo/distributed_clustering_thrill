#!/usr/bin/env ruby

jobs = [
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin", 4, 10, 1],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin", 3, 10, 2],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin", 2, 10, 4],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin", 1, 10, 8],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin", 0, 30, 16],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_16000000-sorted-preprocessed-*.bin", 0, 10, 32],
]

`mkdir clusterings`

BLOCK_SIZE = 1024 * 128

jobs.each do |job|
  graph, hours, minutes, nodes = *job
  puts `msub -v THRILL_WORKERS_PER_HOST=1 -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/mod -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm.sh`
  puts `msub -v THRILL_WORKERS_PER_HOST=1 -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/modnc -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_no_contraction.sh`
  puts `msub -v THRILL_WORKERS_PER_HOST=1 -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/me -l walltime=#{"%02d" % (hours * 2)}:#{"%02d" % (minutes * 2)}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_map_eq.sh`

  puts `msub -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/mod -l walltime=00:15:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm.sh`
  puts `msub -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/modnc -l walltime=00:15:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_no_contraction.sh`
  puts `msub -v THRILL_BLOCK_SIZE=#{BLOCK_SIZE} -v GRAPH=#{graph} -v CLUSTERING=clusterings/me -l walltime=00:30:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_map_eq.sh`
end
