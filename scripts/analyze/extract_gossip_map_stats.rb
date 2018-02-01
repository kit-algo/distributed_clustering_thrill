#!/usr/bin/env ruby

# mpirun -n 8 --bind-to core --map-by node:PE=16 -report-bindings /home/kit/iti/ji4215/gossip_map/release/apps/GossipMap/GossipMap --format bintsv4 --graph /work/kit/iti/ji4215/gossip_map_graphs/uk-2002.metis.bintsv4 --prefix clusterings/uk-2002.metis-13966975 --ncpus 16
# Elapsed time for finding Communities = 963.367

def fix_graph_path gossip_map_graph
  "/home/kit/iti/ji4215/graphs/#{gossip_map_graph[39..-9]}-preprocessed-*.bin"
end

gossip_map = false
cmd_pattern = /mpirun -n (?'hosts'\d+) --bind-to core --map-by node:PE=\d+ -report-bindings (?'bin'\S+GossipMap) --format bintsv4 --graph (?'graph'\S+) --prefix (?'clustering'\S+) --ncpus (?'threads'\d+)/
time_pattern = /Elapsed time for finding Communities = (\d+\.\d+)/
node_pattern = /vertices: (\d+), # edges: \d+/
job_id = (/job_uc1_(\d+)\.out/.match(ARGV[0]) || exit)[1]
File.readlines(ARGV[0]).each do |line|
  match = cmd_pattern.match(line)

  if match
    gossip_map = true
    puts "#LOG# program_run/0/binary: #{match[:bin]}"
    puts "#LOG# program_run/0/hosts: #{match[:hosts]}"
    puts "#LOG# program_run/0/total_workers: #{match[:threads].to_i * match[:hosts].to_i}"
    puts "#LOG# program_run/0/workers_per_host: #{match[:threads]}"
    puts "#LOG# program_run/0/graph: #{fix_graph_path match['graph']}"
    puts "#LOG# program_run/0/job_id: #{job_id}"
    puts "#LOG# algorithm_run/1/program_run_id: 0"
    puts "#LOG# algorithm_run/1/algorithm: gossip map"
    puts "#LOG# clustering/2/algorithm_run_id: 1"
    puts "#LOG# clustering/2/source: computation"
    puts "#LOG# clustering/2/path: #{match[:clustering]}_1.*.bin"
    next
  end

  if gossip_map
    match = node_pattern.match(line)
    if match
      puts "#LOG# program_run/0/node_count: #{match[1]}"
      next
    end

    match = time_pattern.match(line)
    if match
      puts "#LOG# algorithm_run/1/runtime: #{match[1]}"
      next
    end
  end
end
