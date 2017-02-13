#!/usr/bin/env ruby

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph'))

logging_id_counter = -1

def get_unused_logging_id
  logging_id_counter += 1
end

graphs.each do |graph|
  [4, 32, 128, 1024].each do |k|
    logging_id = get_unused_logging_id
    start = Time.now
    `./kaffpa #{graph} -k=#{k} --preconfiguration=ecosocial --output_filename=#{graph}#{k}.kahip.part`
    puts "program_run/#{logging_id}/program_run/time: #{Time.now - start}"
    puts "program_run/#{logging_id}/program_run/binary: kaffpa"
    puts "program_run/#{logging_id}/program_run/graph: #{graph}"
    puts "program_run/#{logging_id}/program_run/output: #{graph}#{k}.kahip.part"

    logging_id = get_unused_logging_id
    start = Time.now
    `./label_prop_partitioning #{graph} -k #{k} -o #{graph}#{k}.lp.part`
    puts "program_run/#{logging_id}/program_run/time: #{Time.now - start}"
    puts "program_run/#{logging_id}/program_run/binary: label_prop_partitioning"
    puts "program_run/#{logging_id}/program_run/graph: #{graph}"
    puts "program_run/#{logging_id}/program_run/output: #{graph}#{k}.lp.part"
  end
end
