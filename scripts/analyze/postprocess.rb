#!/usr/bin/env ruby

scripts_dir = File.expand_path(File.dirname(__FILE__))


Dir.chdir(ARGV.first) do
  puts 'fixing collapse notifications in thrill output'
  puts `sed -i -e ':a;N;$!ba;s/\\[worker [[:digit:]]\\+ [[:digit:]]\\+\\] Collapse rejected File from parent due to non-empty function stack\\.\\n//g' job_uc1_*`

  puts 'generate reports'
  Dir.glob("job_*.out").each do |job_file|
    `#{scripts_dir}/report_to_json.rb #{job_file}`
    `#{scripts_dir}/extract_gossip_map_stats.rb #{job_file} | #{scripts_dir}/report_to_json.rb`
    `#{scripts_dir}/extract_relax_map_stats.rb #{job_file} | #{scripts_dir}/report_to_json.rb`
  end

  puts 'done'
end
