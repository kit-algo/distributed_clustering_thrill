#!/usr/bin/env ruby

require 'securerandom'

algos = ['seq_louvain', 'dlslm_map_eq', 'dlslm_with_seq', 'dlslm', 'infomap']

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph'))
algos.each do |algo|
  `mkdir #{ARGV[1]}/#{algo}`
end

graphs.each do |graph|
  name = graph.split('/').last.split('.')[0..-2].join('.')

  3.times do |i|
    algos.each do |algo|
      if algo == 'seq_louvain' || algo == 'infomap'
        cmd = "./#{algo} #{graph} -o #{ARGV[1]}/#{algo}/#{name}.run#{i}.part.bin"
      else
        cmd = "./#{algo} #{graph} #{ARGV[1]}/#{algo}/#{name}.run#{i}-@@@@-#####.bin,#{SecureRandom.uuid}"
      end
      puts cmd
      begin
        system "#{cmd} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[1]} ./report_to_json.rb"
      rescue Exception => _
      end
    end
  end
end
