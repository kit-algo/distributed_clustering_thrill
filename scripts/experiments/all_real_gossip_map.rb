#!/usr/bin/env ruby

jobs = [
#  ["#{ENV['HOME']}/graphs/uk-2002.metis-preprocessed-*.bin", 1, 0, 8],
#  ["#{ENV['HOME']}/graphs/uk-2007-05.metis-preprocessed-*.bin", 3, 0, 16],
#  ["#{ENV['HOME']}/graphs/in-2004.metis-preprocessed-*.bin", 1, 0, 4],
#  ["#{ENV['HOME']}/graphs/com-friendster-preprocessed-*.bin", 1, 0, 16],
#  ["#{ENV['HOME']}/graphs/com-lj.ungraph-preprocessed-*.bin", 1, 0, 8],
#  ["#{ENV['HOME']}/graphs/com-orkut.ungraph-preprocessed-*.bin", 1, 0, 8],
  ["#{ENV['HOME']}/graphs/com-youtube.ungraph-preprocessed-*.bin", 0, 10, 2],
#  ["#{ENV['HOME']}/graphs/com-amazon.ungraph-preprocessed-*.bin", 0, 10, 2],
#  ["#{ENV['HOME']}/graphs/europe.osm-preprocessed-*.bin", 1, 0, 8],
]

`mkdir clusterings`
`mkdir #{ENV['WORK']}/gossip_map_graphs`

jobs.each do |job|
  graph, hours, minutes, nodes = *job
  graph_name = graph.split("/").last[0..-20]
  gossip_map_file = "#{ENV['WORK']}/gossip_map_graphs/#{graph_name}.bintsv4"

  if File.exist? gossip_map_file
    `touch #{gossip_map_file}`
    puts `msub -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 4)}:00 -l nodes=#{nodes}:ppn=28 -v GRAPH=#{gossip_map_file} -v OUT_PREFIX=clusterings/#{graph_name} ~/code/scripts/moab/gossip_map.sh`
  else
    conversion_job_id = `msub -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 4)}:00 -v GRAPH=#{graph} -v OUT=#{gossip_map_file} ~/code/scripts/moab/gossip_map_conversion.sh`
    puts `msub -l depend=afterok:#{conversion_job_id} -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 4)}:00 -v GRAPH=#{gossip_map_file} -v OUT_PREFIX=clusterings/#{graph_name} ~/code/scripts/moab/gossip_map.sh`
  end
end
