#!/usr/bin/env ruby

jobs = [
  ["#{ENV['HOME']}/graphs/uk-2002.metis-preprocessed-*.bin", 1, 0, true],
  ["#{ENV['HOME']}/graphs/uk-2007-05.metis-preprocessed-*.bin", 3, 0, false],
  # ["#{ENV['HOME']}/graphs/in-2004.metis-preprocessed-*.bin", 1, 0, true],
  ["#{ENV['HOME']}/graphs/com-friendster-preprocessed-*.bin", 2, 0, false],
  ["#{ENV['HOME']}/graphs/com-lj.ungraph-preprocessed-*.bin", 1, 0, true],
  ["#{ENV['HOME']}/graphs/com-orkut.ungraph-preprocessed-*.bin", 1, 0, true],
  # ["#{ENV['HOME']}/graphs/com-youtube.ungraph-preprocessed-*.bin", 0, 10, true],
  # ["#{ENV['HOME']}/graphs/com-amazon.ungraph-preprocessed-*.bin", 0, 10, true],
  # ["#{ENV['HOME']}/graphs/europe.osm-preprocessed-*.bin", 1, 0, true],
]

`mkdir clusterings`

jobs.each do |job|
  graph, hours, minutes, include_mapeq = *job
  puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/louvain -l walltime=#{"%02d" % (hours * 2)}:#{"%02d" % minutes}:00 #{ENV['HOME']}/code/scripts/moab/seq_louvain.sh`
  puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/plm -l walltime=#{"%02d" % (hours)}:#{"%02d" % (minutes)}:00 #{ENV['HOME']}/code/scripts/moab/plm.sh`
  if include_mapeq
    puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/infomap -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 2)}:00 #{ENV['HOME']}/code/scripts/moab/seq_infomap.sh`
    puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/infomap)directed -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 2)}:00 #{ENV['HOME']}/code/scripts/moab/seq_infomap_directed.sh`
    puts `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/relaxmap -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 2)}:00 #{ENV['HOME']}/code/scripts/moab/relax_map.sh`
  end
end
