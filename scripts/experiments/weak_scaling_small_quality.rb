#!/usr/bin/env ruby

require 'securerandom'

algos = ['dlslm_map_eq', 'dlslm_no_contraction', 'dlslm']


algos.each do |algo|
  `mkdir data/results/weak_scaling_small_quality/#{algo}`
  `mkdir data/results/weak_scaling_small_quality/#{algo}/clusterings`
end

(0..3).map { |exp| 2**exp }.each do |million_nodes|
  name = "graph_50_10000_mu_0.4_#{million_nodes}000000-sorted-preprocessed"
  bin_graph_path = "data/graphs/mu-04/#{name}-*.bin"

  10.times do |i|
    algos.each do |algo|
      cmd = "./#{algo} '#{bin_graph_path}' data/results/weak_scaling_small_quality/#{algo}/clusterings/#{name}.run#{i}-@@@@-#####.bin,#{SecureRandom.uuid}"
      puts cmd
      begin
        system "#{cmd} | tee data/results/weak_scaling_small_quality/#{algo}/job_#{name}_#{i}.out"
      rescue Exception => _
      end
    end
  end
end
