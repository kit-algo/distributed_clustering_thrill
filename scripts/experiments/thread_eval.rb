#!/usr/bin/env ruby

graphs = [
  'graphs/graph_50_10000_mu_0.5_1000000-preprocessed-*.bin',
  'graphs/graph_50_10000_mu_0.5_10000000-preprocessed-*.bin'
]

node_configs = [
  '-l nodes=1:ppn=2 -v THRILL_WORKERS_PER_HOST=2 -l pmem=32000mb -q singlenode',
  '-l nodes=1:ppn=3 -v THRILL_WORKERS_PER_HOST=3 -l pmem=21000mb -q singlenode',
  '-l nodes=1:ppn=5 -v THRILL_WORKERS_PER_HOST=5 -l pmem=12500mb -q singlenode',
  '-l nodes=1:ppn=9 -v THRILL_WORKERS_PER_HOST=9 -l pmem=7100mb -q singlenode',
  '-l nodes=1:ppn=16 -v THRILL_WORKERS_PER_HOST=16 -l pmem=4000mb -q singlenode',
  '-l nodes=4:ppn=2 -v THRILL_WORKERS_PER_HOST=2 -l pmem=32000mb -q multinode',
  '-l nodes=4:ppn=3 -v THRILL_WORKERS_PER_HOST=3 -l pmem=21000mb -q multinode',
  '-l nodes=4:ppn=5 -v THRILL_WORKERS_PER_HOST=5 -l pmem=12500mb -q multinode',
  '-l nodes=4:ppn=9 -v THRILL_WORKERS_PER_HOST=9 -l pmem=7100mb -q multinode',
  '-l nodes=4:ppn=16 -v THRILL_WORKERS_PER_HOST=16 -l pmem=4000mb -q multinode',
]


graphs.each do |graph|
  node_configs.each do |config|
    command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/dlm -l walltime=01:00:00 #{config} ~/code/scripts/moab/no_default_dlm.sh`
    puts "#{graph} #{configs[0]} #{node_config} #{command_result.strip}"
  end
end
