#!/usr/bin/env ruby

graphs = {
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000-sorted-preprocessed-*.bin" => :fast,
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_1000000-preprocessed-*.bin" => :standard,
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin" => :slow,
  "#{ENV['HOME']}/graphs/hypercubegraph23-preprocessed-*.bin" => :slow,
  "#{ENV['HOME']}/graphs/uk-2002.metis-preprocessed-*.bin" => :slow,
  "#{ENV['HOME']}/graphs/uk-2007-05.metis-preprocessed-*.bin" => :slow,
  "#{ENV['HOME']}/graphs/in-2004.metis-preprocessed-*.bin" => :standard,
  "#{ENV['HOME']}/graphs/com-friendster-preprocessed-*.bin" => :slow,
  "#{ENV['HOME']}/graphs/com-lj.ungraph-preprocessed-*.bin" => :standard,
  "#{ENV['HOME']}/graphs/com-orkut.ungraph-preprocessed-*.bin" => :standard,
  "#{ENV['HOME']}/graphs/com-youtube.ungraph-preprocessed-*.bin" => :fast,
  "#{ENV['HOME']}/graphs/com-amazon.ungraph-preprocessed-*.bin" => :fast
}

time_configs = {
  fast: '-l walltime=00:10:00',
  standard: '-l walltime=00:10:00',
  slow: '-l walltime=03:00:00'
}


graphs.each do |graph, config|
  command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/seq #{time_configs[config]} #{ENV['HOME']}/code/scripts/moab/seq_louvain.sh`
  puts "#{graph} #{time_configs[config]} #{command_result.strip}"
end
