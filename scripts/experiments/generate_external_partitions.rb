#!/usr/bin/env ruby

logging_id_counter = 0

ARGV.each do |graph|
  [4, 16, 64, 256, 1024].each do |k|
    start = Time.now
    system "./label_prop #{graph} -k #{k} -o #{File.join(File.dirname(graph), 'partitionings', "#{graph}#{k}.lp.part")}"
    puts "#LOG# program_run/#{logging_id_counter}/runtime: #{Time.now - start}"
    puts "#LOG# program_run/#{logging_id_counter}/binary: label_prop_partitioning"
    puts "#LOG# program_run/#{logging_id_counter}/graph: #{graph}"
    puts "#LOG# program_run/#{logging_id_counter}/output: #{File.join(ARGV[1], "#{graph}#{k}.lp.part")}"
    logging_id_counter += 1
  end
end
