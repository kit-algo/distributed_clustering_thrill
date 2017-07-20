#!/usr/bin/env ruby

graphs = {
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000-sorted-preprocessed-*.bin" => [:fast, [:s]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_1000000-preprocessed-*.bin" => [:fast, [:m]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin" => [:standard, [:l]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000000-preprocessed-*.bin" => [:slow, [:xl]],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_200000000-sorted-preprocessed-*.bin" => [:slow, [:xxl]],
  "#{ENV['HOME']}/graphs/uk-2007-05.metis-preprocessed-*.bin" => [:slow, [:xl]],
}

time_configs = {
  fast: '-l walltime=00:10:00',
  standard: '-l walltime=01:00:00',
  slow: '-l walltime=05:00:00'
}

node_configs = {
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
      moab = 'no_default_dlslm_map_eq.sh'
      command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/sme #{time_configs[configs[0]]} #{node_configs[node_config]} ~/code/scripts/moab/#{moab}`
      puts "#{graph} #{configs[0]} #{node_config} #{command_result.strip}"
    end
  end
end
