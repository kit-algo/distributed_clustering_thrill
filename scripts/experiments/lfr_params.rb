#!/usr/bin/env ruby

require 'securerandom'

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph')) # + Dir.glob(File.join(ARGV[0], '**/*.txt'))
`mkdir #{ARGV[1]}/seq_louvain`
`mkdir #{ARGV[1]}/dlm`

graphs.each do |graph|
  name = graph.split('/').last.split('.').first
  2.times do |i|
    cmd = "./seq_louvain #{graph} -o #{ARGV[1]}/seq_louvain/#{name}.run#{i}.part"
    puts cmd
    begin
      system "#{cmd} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[1]} ./report_to_json.rb"
    rescue Exception => _
    end
  end

  cmd = "./node_based_local_moving #{graph} #{ARGV[1]}/dlm/#{name}.part,#{SecureRandom.uuid}"
  puts cmd
  begin
    system "#{cmd} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[1]} ./report_to_json.rb"
  rescue Exception => _
  end
end
