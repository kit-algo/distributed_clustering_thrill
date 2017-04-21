#!/usr/bin/env ruby

require 'json'


ARGV.each do |job_output_file|
  job_id = job_output_file.match(/job_uc1_(\d+)\.out/)[1]

  start = Float::INFINITY
  final_stage = 0
  ending = 0

  Dir.glob(File.join(File.dirname(job_output_file), "*-log-#{job_id}-*.json")).each do |json_log|
    File.open(json_log).each do |line|
      event = JSON.parse(line)
      start = event['ts'] if event['id'] == 9 && event['event'] == 'execute-start' && event['ts'] < start
      if event['id'] && event['id'] >= final_stage && event['label'] == 'ZipWithIndex' && event['event'] == 'pushdata-done'
        final_stage = event['id']
        ending = event['ts'] if event['ts'] > ending
      end
    end
  end

  open(job_output_file, 'a') do |f|
    f.puts "#LOG# algorithm_run/1/runtime: #{(ending - start) / 1000}"
  end
end
