#!/usr/bin/env ruby

graphs = {
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_1000000-preprocessed-*.bin" => [:xs, :s],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin" => [:s, :m, :l],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000000-preprocessed-*.bin" => [:l, :xl],
}


node_configs = {
  xs: [
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=28',
  ],
  s: [
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=28',
  ],
  m: [
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=28',
  ],
  l: [
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=28',
  ],
  xl: [
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=2 ',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=5 ',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=17',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=28',
  ],
}


graphs.each do |graph, graph_configs|
  graph_configs.each do |size|
    node_configs[size].each do |config|
      moab = (size == :xs ? 'fake_single_dlm.sh' : 'no_default_dlm.sh')
      command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/dlm -l walltime=01:00:00 #{config} #{ENV['HOME']}/code/scripts/moab/#{moab}`
      puts "#{graph} #{config} #{command_result.strip}"
    end
  end
end
