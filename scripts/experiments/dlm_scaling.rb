#!/usr/bin/env ruby

graphs = {
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_1000000-preprocessed-*.bin" => [:xs, :s],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_10000000-preprocessed-*.bin" => [:s, :m, :l],
  "#{ENV['HOME']}/graphs/mu-04/graph_50_10000_mu_0.4_100000000-preprocessed-*.bin" => [:l, :xl],
}


node_configs = {
  xs: [
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l pmem=4500mb -q multinode',
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l pmem=4500mb -q multinode',
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l pmem=4500mb -q multinode',
    '-l nodes=1:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l pmem=4500mb -q multinode',
  ],
  s: [
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l pmem=4500mb -q multinode',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l pmem=4500mb -q multinode',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l pmem=4500mb -q multinode',
    '-l nodes=2:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l pmem=4500mb -q multinode',
  ],
  m: [
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l pmem=4500mb -q multinode',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l pmem=4500mb -q multinode',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l pmem=4500mb -q multinode',
    '-l nodes=4:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l pmem=4500mb -q multinode',
  ],
  l: [
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l pmem=4500mb -q multinode',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l pmem=4500mb -q multinode',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l pmem=4500mb -q multinode',
    '-l nodes=8:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l pmem=4500mb -q multinode',
  ],
  xl: [
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=2  -l pmem=4500mb -q multinode',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=5  -l pmem=4500mb -q multinode',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=17 -l pmem=4500mb -q multinode',
    '-l nodes=16:ppn=28 -v THRILL_WORKERS_PER_HOST=28 -l pmem=4500mb -q multinode',
  ],
}


graphs.each do |graph, graph_configs|
  graph_configs.each do |size|
    node_configs[size].each do |config|
      command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/dlm -l walltime=01:00:00 #{config} #{ENV['HOME']}/code/scripts/moab/no_default_dlm.sh`
      puts "#{graph} #{config} #{command_result.strip}"
    end
  end
end
