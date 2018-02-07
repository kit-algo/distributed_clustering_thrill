#!/usr/bin/env ruby

# /home/kit/iti/ji4215/RelaxMap/ompRelaxmap 719706760 /home/kit/iti/ji4215/graphs/uk-2002.metis-preprocessed-*.bin 16 1 1e-3 0.0 10 clusterings/relaxmap-14399906.part.bin prior
# done! (found 18483186 nodes and 523574516 edges.)
# Overall Elapsed Time for Module Detection (w/o file IO): 1084.11 (sec)

relax_map = false
cmd_pattern = /(?'bin'\S+ompRelaxmap) (?'seed'\d+) (?'graph'\S+) (?'threads'\d+) 1 1e-3 0\.0 10 (?'clustering'\S+) prior/
node_pattern = /done! \(found (\d+) nodes and \d+ edges\.\)/
time_pattern = /Overall Elapsed Time for Module Detection \(w\/o file IO\): (\d+\.\d+) \(sec\)/
job_id = (/job_uc1_(\d+)\.out/.match(ARGV[0]) || exit)[1]
File.readlines(ARGV[0]).each do |line|
  match = cmd_pattern.match(line)

  if match
    relax_map = true
    puts "#LOG# program_run/0/binary: #{match[:bin]}"
    puts "#LOG# program_run/0/seed: #{match[:seed]}"
    puts "#LOG# program_run/0/hosts: 1"
    puts "#LOG# program_run/0/total_workers: #{match[:threads]}"
    puts "#LOG# program_run/0/workers_per_host: #{match[:threads]}"
    puts "#LOG# program_run/0/graph: #{match['graph']}"
    puts "#LOG# program_run/0/job_id: #{job_id}"
    puts "#LOG# algorithm_run/1/program_run_id: 0"
    puts "#LOG# algorithm_run/1/algorithm: relax map"
    puts "#LOG# clustering/2/algorithm_run_id: 1"
    puts "#LOG# clustering/2/source: computation"
    puts "#LOG# clustering/2/path: #{match[:clustering]}"
    next
  end

  if relax_map
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
