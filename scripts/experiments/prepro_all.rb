#!/usr/bin/env ruby

# [
#   ['graphs/graph_50_10000_mu_0.5_100000.bin', 'graphs/part_50_10000_mu_0.5_100000.bin'],
#   ['graphs/graph_50_10000_mu_0.5_1000000.bin', 'graphs/part_50_10000_mu_0.5_1000000.bin'],
#   ['graphs/graph_50_10000_mu_0.5_10000000.bin', 'graphs/part_50_10000_mu_0.5_10000000.bin'],
#   ['graphs/graph_50_10000_mu_0.5_100000000.bin', 'graphs/part_50_10000_mu_0.5_100000000.bin'],
#   ['graphs/hypercubegraph23.graph'],
#   ['graphs/uk-2002.metis.graph'],
#   ['graphs/uk-2007-05.metis.graph'],
#   ['graphs/in-2004.metis.graph'],
#   ['graphs/com-friendster.txt'],
#   ['graphs/com-lj.ungraph.txt'],
#   ['graphs/com-orkut.ungraph.txt'],
#   ['graphs/com-youtube.ungraph.txt'],
#   ['graphs/com-amazon.ungraph.txt']
# ]

graphs = [
  # ['graphs/hypercubegraph23.graph'],
  # ['graphs/uk-2002.metis.graph'],
  # ['graphs/uk-2007-05.metis.graph'],
  # ['graphs/com-friendster.txt'],
  # ['graphs/com-lj.ungraph.txt'],
  # ['graphs/com-orkut.ungraph.txt']
  # ['graphs/mu-04/graph_50_10000_mu_0.4_10000000.bin', 'graphs/mu-04/part_50_10000_mu_0.4_10000000.bin']
]

small_graphs = [
  # ['graphs/graph_50_10000_mu_0.5_100000.bin', 'graphs/part_50_10000_mu_0.5_100000.bin'],
  # ['graphs/graph_50_10000_mu_0.5_1000000.bin', 'graphs/part_50_10000_mu_0.5_1000000.bin'],
  # ['graphs/in-2004.metis.graph'],
  # ['graphs/com-youtube.ungraph.txt'],
  # ['graphs/com-amazon.ungraph.txt']
  # ['graphs/mu-04/graph_50_10000_mu_0.4_100000-sorted*.bin', 'graphs/mu-04/part_50_10000_mu_0.4_100000-sorted.bin'],
  # ['graphs/mu-04/graph_50_10000_mu_0.4_1000000.bin', 'graphs/mu-04/part_50_10000_mu_0.4_1000000.bin']
]

large_graphs = [
  # ['graphs/mu-04/graph_50_10000_mu_0.4_100000000.bin', 'graphs/mu-04/part_50_10000_mu_0.4_100000000.bin'],
  # ['graphs/mu-04/graph_50_10000_mu_0.4_200000000-sorted.bin', 'graphs/mu-04/part_50_10000_mu_0.4_200000000-sorted.bin'],
  ["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_1000000000-sorted*.bin", "#{ENV['WORK']}/graphs/part_50_10000_mu_0.4_1000000000-sorted.bin"],
]

graphs.each do |graph|
  groundtruth_option = graph.size == 2 ? "-v GROUNDTRUTH=#{ENV['HOME']}/#{graph[1]}" : ''
  puts graph[0], `msub -v GRAPH=#{ENV['HOME']}/#{graph[0]} #{groundtruth_option} ~/code/scripts/moab/preprocess.sh`
end

small_graphs.each do |graph|
  groundtruth_option = graph.size == 2 ? "-v GROUNDTRUTH=#{ENV['HOME']}/#{graph[1]}" : ''
  puts graph[0], `msub -v GRAPH=#{ENV['HOME']}/#{graph[0]} #{groundtruth_option} ~/code/scripts/moab/preprocess_s.sh`
end

large_graphs.each do |graph|
  groundtruth_option = graph.size == 2 ? "-v GROUNDTRUTH=#{ENV['WORK']}/#{graph[1]}" : ''
  puts graph[0], `msub -v GRAPH=#{ENV['WORK']}/#{graph[0]} #{groundtruth_option} ~/code/scripts/moab/preprocess_l.sh`
end
