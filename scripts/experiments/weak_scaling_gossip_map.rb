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
`mkdir #{ENV['WORK']}/gossip_map_graphs`

jobs.each do |job|
  graph, hours, minutes, nodes = *job
  graph_name = graph.split("/").last[0..-20]
  gossip_map_file = "#{ENV['WORK']}/gossip_map_graphs/#{graph_name}.bintsv4"

  depend = ''
  if File.exist? gossip_map_file
    `touch #{gossip_map_file}`
  else
    conversion_job_id = `msub -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 4)}:00 -v GRAPH=#{graph} -v OUT=#{gossip_map_file} ~/code/scripts/moab/gossip_map_conversion.sh`.strip
    depend = "-l depend=afterok:#{conversion_job_id}"
  end
  puts `msub #{depend} -l walltime=#{"%02d" % (hours * 4)}:#{"%02d" % (minutes * 4)}:00 -l nodes=#{nodes}:ppn=28 -v GRAPH=#{gossip_map_file} -v OUT_PREFIX=clusterings/#{graph_name} ~/code/scripts/moab/gossip_map.sh`
end
