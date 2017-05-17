#!/usr/bin/env ruby

logging_id_counter = 0

ARGV.each do |graph|
  [4, 16, 64, 256, 1024].each do |k|
    start = Time.now
    output = File.join(File.dirname(Dir.glob(graph).first), 'partitionings', "#{graph.gsub('*', '')}#{k}.lp.part")
    system "./label_prop #{graph} -k #{k} -o #{output}"
    puts "#LOG# program_run/#{logging_id_counter}/runtime: #{Time.now - start}"
    puts "#LOG# program_run/#{logging_id_counter}/binary: label_prop_partitioning"
    puts "#LOG# program_run/#{logging_id_counter}/graph: #{graph}"
    puts "#LOG# program_run/#{logging_id_counter}/output: #{output}"
    logging_id_counter += 1
  end
end
