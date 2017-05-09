#!/usr/bin/env ruby

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph'))

logging_id_counter = 0

graphs.each do |graph|
  [4, 32, 128, 1024].each do |k|
    start = Time.now
    `./kaffpa #{graph} --k=#{k} --preconfiguration=ecosocial --output_filename=#{graph}#{k}.kahip.part`
    puts "#LOG# program_run/#{logging_id_counter}/time: #{Time.now - start}"
    puts "#LOG# program_run/#{logging_id_counter}/binary: kaffpa"
    puts "#LOG# program_run/#{logging_id_counter}/graph: #{graph}"
    puts "#LOG# program_run/#{logging_id_counter}/output: #{graph}#{k}.kahip.part"
    logging_id_counter += 1

    start = Time.now
    `./label_prop_partitioning #{graph} -k #{k} -o #{graph}#{k}.lp.part`
    puts "#LOG# program_run/#{logging_id_counter}/time: #{Time.now - start}"
    puts "#LOG# program_run/#{logging_id_counter}/binary: label_prop_partitioning"
    puts "#LOG# program_run/#{logging_id_counter}/graph: #{graph}"
    puts "#LOG# program_run/#{logging_id_counter}/output: #{graph}#{k}.lp.part"
    logging_id_counter += 1
  end
end
