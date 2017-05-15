#!/usr/bin/env ruby

graphs = [
  "#{ENV['HOME']}/graphs/uk-2002.metis-preprocessed-*.bin",
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin",
  "#{ENV['HOME']}/graphs/com-friendster-preprocessed-*.bin",
]


node_configs = [
  '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
  '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
  '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
  '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
  '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
  '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
  '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
  '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
  '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
]


graphs.each do |graph, graph_configs|
  graph_configs.each do |size|
    node_configs[size].each do |config|
      command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/plm -l walltime=01:00:00 #{config} #{ENV['HOME']}/code/scripts/moab/no_default_plm.sh`
      puts "#{graph} #{config} #{command_result.strip}"
    end
  end
end
