#!/usr/bin/env ruby

require 'json'


ARGV.each_with_index do |job_output_file, i|
  puts "#{i}/#{ARGV.size}"
  next unless `tail -n1 #{job_output_file}`.start_with?('malloc_tracker ### exiting')

  job_id = job_output_file.match(/job_uc1_(\d+)\.out/)[1]

  start = Float::INFINITY
  final_stage = 0
  ending = 0

  Dir.glob(File.join(File.dirname(job_output_file), "lp-log-#{job_id}-*.json")).each do |json_log|
    File.open(json_log).each do |line|
      begin
        event = JSON.parse(line)
        start = event['ts'] if event['id'] == 7 && event['event'] == 'pushdata-done' && event['ts'] < start
        if event['id'] && event['id'] >= final_stage && event['label'] == 'ReduceToIndex' && event['event'] == 'execute-done'
          final_stage = event['id']
          ending = event['ts'] if event['ts'] > ending
        end
      rescue JSON::ParserError
      end
    end
  end

  if start != Float::INFINITY && ending != 0
    open(job_output_file, 'a') do |f|
      f.puts "#LOG# program_run/0/runtime: #{(ending - start) / 1000000.0}"
    end
  end
end
