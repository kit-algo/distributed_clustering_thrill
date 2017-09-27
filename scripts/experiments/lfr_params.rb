#!/usr/bin/env ruby

require 'securerandom'

algos = ['seq_louvain', 'dlslm_map_eq', 'dlslm_with_seq', 'dlslm']

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph'))
algos.each do |algo|
  `mkdir #{ARGV[1]}/#{algo}`
end

graphs.each do |graph|
  name = graph.split('/').last.split('.').first

  3.times do |i|
    algos.each do |algo|
      if algo == 'seq_louvain'
        cmd = "./#{algo} #{graph} -o #{ARGV[1]}/#{algo}/#{name}.run#{i}.part"
      else
        cmd = "./#{algo} #{graph} #{ARGV[1]}/#{algo}/#{name}.run#{i}.part,#{SecureRandom.uuid}"
      end
      puts cmd
      begin
        system "#{cmd} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[1]} ./report_to_json.rb"
      rescue Exception => _
      end
    end
  end
end
