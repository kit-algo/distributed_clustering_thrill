#!/usr/bin/env ruby

graphs = {
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000-sorted-preprocessed-*.bin" => [:fast, [:xs, :s]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_1000000-preprocessed-*.bin" => [:fast, [:xs, :s, :m]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin" => [:standard, [:s, :m, :l]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000000-preprocessed-*.bin" => [:slow, [:m, :l, :xl]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_200000000-sorted-preprocessed-*.bin" => [:slow, [:l, :xl, :xxl]],
  "#{ENV['HOME']}/graphs/hypercubegraph23-preprocessed-*.bin" => [:standard, [:s, :m, :l]],
  "#{ENV['HOME']}/graphs/uk-2002.metis-preprocessed-*.bin" => [:standard, [:s, :m, :l]],
  "#{ENV['HOME']}/graphs/uk-2007-05.metis-preprocessed-*.bin" => [:slow, [:m, :l, :xl]],
  "#{ENV['HOME']}/graphs/in-2004.metis-preprocessed-*.bin" => [:standard, [:xs, :s, :m]],
  "#{ENV['HOME']}/graphs/com-friendster-preprocessed-*.bin" => [:standard, [:m, :l, :xl]],
  "#{ENV['HOME']}/graphs/com-lj.ungraph-preprocessed-*.bin" => [:standard, [:s, :m, :l]],
  "#{ENV['HOME']}/graphs/com-orkut.ungraph-preprocessed-*.bin" => [:standard, [:s, :m, :l]],
  "#{ENV['HOME']}/graphs/com-youtube.ungraph-preprocessed-*.bin" => [:fast, [:xs, :s]],
  "#{ENV['HOME']}/graphs/com-amazon.ungraph-preprocessed-*.bin" => [:fast, [:xs, :s]]
}

time_configs = {
  fast: '-l walltime=00:10:00',
  standard: '-l walltime=01:00:00',
  slow: '-l walltime=05:00:00'
}

node_configs = {
  xs: '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
  s:  '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
  m:  '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
  l:  '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
  xl: '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
  xxl: '-l nodes=32:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
}


graphs.each do |graph, configs|
  ARGV.each do |prio|
    node_config = configs[1][-prio.to_i]
    if node_config
      moab = (size == :xs ? 'fake_single_dlm.sh' : 'no_default_dlm.sh')
      command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/dlm #{time_configs[configs[0]]} #{node_configs[node_config]} ~/code/scripts/moab/#{moab}`
      puts "#{graph} #{configs[0]} #{node_config} #{command_result.strip}"
    end
  end
end
