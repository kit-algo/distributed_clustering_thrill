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

jobs = [
  [["#{ENV['WORK']}/graphs/graph_50_10000_mu_0.4_1000000-sorted*.bin", "#{ENV['WORK']}/graphs/part_50_10000_mu_0.4_1000000-sorted.bin"], '-l walltime=00:10:00', '-l nodes=2:ppn=28'],
]

jobs.each do |job|
  groundtruth_option = job[0].size == 2 ? "-v GROUNDTRUTH=#{job[0][1]}" : ''
  puts job[0][0], `msub -v GRAPH=#{job[0][0]} #{groundtruth_option} #{job[1]} #{job[2]} ~/code/scripts/moab/preprocess.sh`
end
