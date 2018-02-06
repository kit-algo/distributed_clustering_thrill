#!/usr/bin/env ruby

require 'securerandom'

algos = ['seq_louvain', 'dlslm_map_eq', 'dlslm_no_contraction', 'dlslm', 'infomap', 'ompRelaxmap', 'plm', 'gossip_map']

algos.each do |algo|
  `mkdir data/results/lfr_params/#{algo}`
  `mkdir data/results/lfr_params/#{algo}/clusterings`
end

(1..9).each do |mu|
  name = "graph_g2_b1_m0.#{mu}"
  bin_graph_path = "data/graphs/lfr_params/#{name}-preprocessed-*.bin"

  10.times do |i|
    algos.each do |algo|
      if algo == 'seq_louvain' || algo == 'infomap'
        cmd = "./#{algo} -b '#{bin_graph_path}' -o data/results/lfr_params/#{algo}/clusterings/#{name}.run#{i}.part.bin"
      elsif algo == 'ompRelaxmap'
        seed = rand 2**31 - 1
        cmd = "./#{algo} #{seed} #{bin_graph_path} 16 1 1e-3 0.0 10 data/results/lfr_params/#{algo}/clusterings/#{name}.run#{i}.part.bin prior"
      elsif algo == 'plm'
        cmd = "./#{algo} #{bin_graph_path} data/results/lfr_params/#{algo}/clusterings/#{name}.run#{i}.part.txt"
      elsif algo == 'gossip_map'
        cmd = "./#{algo} --format bintsv4 --graph data/graphs/lfr_params/#{name}.bintsv4 --prefix data/results/lfr_params/#{algo}/clusterings/#{name}.run#{i} --ncpus 4"
      else
        cmd = "./#{algo} #{bin_graph_path} data/results/lfr_params/#{algo}/clusterings/#{name}.run#{i}-@@@@-#####.bin,#{SecureRandom.uuid}"
      end
      puts cmd
      begin
        system "#{cmd} | tee data/results/lfr_params/#{algo}/job_#{name}_#{i}.out"
      rescue Exception => _
      end
    end
  end
end
