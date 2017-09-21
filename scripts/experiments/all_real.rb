#!/usr/bin/env ruby

jobs = [
  ["#{ENV['HOME']}/graphs/uk-2002.metis-preprocessed-*.bin" 1, 0, 8],
  ["#{ENV['HOME']}/graphs/uk-2007-05.metis-preprocessed-*.bin", 3, 0, 16],
  ["#{ENV['HOME']}/graphs/in-2004.metis-preprocessed-*.bin" 1, 0, 4],
  ["#{ENV['HOME']}/graphs/com-friendster-preprocessed-*.bin", 1, 0, 16],
  ["#{ENV['HOME']}/graphs/com-lj.ungraph-preprocessed-*.bin", 1, 0, 8],
  ["#{ENV['HOME']}/graphs/com-orkut.ungraph-preprocessed-*.bin", 1, 0, 8],
  ["#{ENV['HOME']}/graphs/com-youtube.ungraph-preprocessed-*.bin", 0, 10, 2],
  ["#{ENV['HOME']}/graphs/com-amazon.ungraph-preprocessed-*.bin", 0, 10, 2],
  ["#{ENV['HOME']}/graphs/europe.osm-preprocessed-*.bin", 1, 0, 2],
]

jobs.each do |job|
  graph, hours, minutes, nodes = *job
  puts `msub -v GRAPH=#{graph} -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm.sh`
  puts `msub -v GRAPH=#{graph} -l walltime=#{"%02d" % hours}:#{"%02d" % minutes}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_with_seq.sh`
  puts `msub -v GRAPH=#{graph} -l walltime=#{"%02d" % (hours * 2)}:#{"%02d" % (minutes * 2)}:00 -l nodes=#{[nodes, 2].max}:ppn=28 #{ENV['HOME']}/code/scripts/moab/#{nodes == 1 ? 'fake_single_' : ''}dlslm_map_eq.sh`
end
