#!/usr/bin/env ruby

graphs = {
  'graphs/graph_50_10000_mu_0.5_1000000-preprocessed-*.bin' => [:s, :m],
  'graphs/graph_50_10000_mu_0.5_10000000-preprocessed-*.bin' => [:s, :m]
}


node_configs = {
  s: [
    '-l nodes=1:ppn=2 -v THRILL_WORKERS_PER_HOST=2 -l mem=64gb -q singlenode',
    '-l nodes=1:ppn=3 -v THRILL_WORKERS_PER_HOST=3 -l mem=64gb -q singlenode',
    '-l nodes=1:ppn=5 -v THRILL_WORKERS_PER_HOST=5 -l mem=64gb -q singlenode',
    '-l nodes=1:ppn=9 -v THRILL_WORKERS_PER_HOST=9 -l mem=64gb -q singlenode',
    '-l nodes=1:ppn=16 -v THRILL_WORKERS_PER_HOST=16 -l mem=64gb -q singlenode',
  ],
  m: [
    '-l nodes=4:ppn=2 -v THRILL_WORKERS_PER_HOST=2 -l mem=256gb -q multinode',
    '-l nodes=4:ppn=3 -v THRILL_WORKERS_PER_HOST=3 -l mem=256gb -q multinode',
    '-l nodes=4:ppn=5 -v THRILL_WORKERS_PER_HOST=5 -l mem=256gb -q multinode',
    '-l nodes=4:ppn=9 -v THRILL_WORKERS_PER_HOST=9 -l mem=256gb -q multinode',
    '-l nodes=4:ppn=16 -v THRILL_WORKERS_PER_HOST=16 -l mem=256gb -q multinode',
  ]
}


graphs.each do |graph, graph_configs|
  graph_configs.each do |size|
    node_configs[size].each do |config|
      command_result = `msub -v GRAPH=#{graph} -v CLUSTERING=clusterings/dlm -l walltime=01:00:00 #{config} ~/code/scripts/moab/no_default_dlm.sh`
      puts "#{graph} #{config} #{command_result.strip}"
    end
  end
end
