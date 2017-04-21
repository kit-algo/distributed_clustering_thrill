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
  ['graphs/uk-2007-05.metis.graph'],
  # ['graphs/com-friendster.txt'],
  # ['graphs/com-lj.ungraph.txt'],
  # ['graphs/com-orkut.ungraph.txt']
]

small_graphs = [
  # ['graphs/graph_50_10000_mu_0.5_100000.bin', 'graphs/part_50_10000_mu_0.5_100000.bin'],
  # ['graphs/graph_50_10000_mu_0.5_1000000.bin', 'graphs/part_50_10000_mu_0.5_1000000.bin'],
  # ['graphs/in-2004.metis.graph'],
  # ['graphs/com-youtube.ungraph.txt'],
  # ['graphs/com-amazon.ungraph.txt']
]

graphs.each do |graph|
  groundtruth_option = graph.size == 2 ? "-v GROUNDTRUTH=#{graph[1]}" : ''
  puts graph[0], `msub -v GRAPH=#{graph[0]} #{groundtruth_option} ~/code/scripts/moab/preprocess.sh`
end

small_graphs.each do |graph|
  groundtruth_option = graph.size == 2 ? "-v GROUNDTRUTH=#{graph[1]}" : ''
  puts graph[0], `msub -v GRAPH=#{graph[0]} #{groundtruth_option} ~/code/scripts/moab/preprocess_s.sh`
end
