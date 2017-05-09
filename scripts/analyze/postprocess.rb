#!/usr/bin/env ruby

scripts_dir = File.expand_path(File.dirname(__FILE__))

Dir.chdir(ARGV.first) do
  `mkdir reports`
  puts 'fix json'
  `sed -i 's/:inf/:Infinity/g;s/:nan/:NaN/g;s/:-nan/:-NaN/g' *.json`
  puts 'extract runtimes'
  `#{scripts_dir}/runtime_extractor.rb #{Dir.glob("job_*.out").join(' ')}`
  puts 'generate reports'
  Dir.glob("job_*.out").each do |job_file|
    `MA_RESULT_OUTPUT_DIR=#{File.join(ARGV.first, 'reports')} #{scripts_dir}/report_to_json.rb #{job_file}`
  end
  puts 'generate zip'
  `zip reports.zip reports/*`
  `rm -r reports`
  puts 'done'
end
