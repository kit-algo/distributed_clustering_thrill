#!/usr/bin/env ruby

graphs = {
  # "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_1000000-preprocessed-*.bin" => [:xs, :s],
  # "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin" => [:s, :m, :l],
  # "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000000-preprocessed-*.bin" => [:ll, :xl],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin" => [:xl],
}


node_configs = {
  xs: [ # actually one node
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l walltime=01:00:00',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l walltime=01:00:00',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l walltime=01:00:00',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l walltime=01:00:00',
  ],
  s: [
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l walltime=01:00:00',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l walltime=01:00:00',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l walltime=01:00:00',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l walltime=01:00:00',
  ],
  m: [
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l walltime=01:00:00',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l walltime=01:00:00',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l walltime=01:00:00',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l walltime=01:00:00',
  ],
  l: [
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l walltime=01:00:00',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l walltime=01:00:00',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l walltime=01:00:00',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l walltime=01:00:00',
  ],
  ll: [
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l walltime=05:00:00',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l walltime=05:00:00',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l walltime=05:00:00',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l walltime=05:00:00',
  ],
  xl: [
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l walltime=03:00:00',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l walltime=02:00:00',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l walltime=02:00:00',
    # '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l walltime=02:00:00',
  ],
}


graphs.each do |graph, graph_configs|
  graph_configs.each do |size|
    node_configs[size].each do |config|
      moab = (size == :xs ? 'fake_single_lp.sh' : 'no_default_lp.sh')
      command_result = `msub -v GRAPH=#{graph} #{config} #{ENV['HOME']}/code/scripts/moab/#{moab}`
      puts "#{graph} #{config} #{command_result.strip}"
    end
  end
end
