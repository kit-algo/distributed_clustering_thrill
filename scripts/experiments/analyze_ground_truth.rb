#!/usr/bin/env ruby

graphs = [
  # ['graphs/mu-04/graph_50_10000_mu_0.4_200000000-sorted-preprocessed-*.bin', 'graphs/mu-04/part_50_10000_mu_0.4_200000000-sorted-preprocessed-*.bin'],
  # ['graphs/mu-04/graph_50_10000_mu_0.4_100000000-preprocessed-*.bin', 'graphs/mu-04/part_50_10000_mu_0.4_100000000-preprocessed-*.bin'],
  ['graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin', 'graphs/mu-04/part_50_10000_mu_0.4_10000000-preprocessed-*.bin'],
  ['graphs/mu-04/graph_50_10000_mu_0.4_1000000-preprocessed-*.bin', 'graphs/mu-04/part_50_10000_mu_0.4_1000000-preprocessed-*.bin'],
  ['graphs/mu-04/graph_50_10000_mu_0.4_100000-sorted-preprocessed-*.bin', 'graphs/mu-04/part_50_10000_mu_0.4_100000-sorted-preprocessed-*.bin']
]

graphs.each do |graph, ground_truth|
  puts graph, `msub -v GRAPH=#{ENV['HOME']}/#{graph} -v GROUNDTRUTH=#{ENV['HOME']}/#{ground_truth} #{ENV['HOME']}/code/scripts/moab/analyze_ground_truth.sh`
end
