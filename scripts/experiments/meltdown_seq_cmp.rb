#!/usr/bin/env ruby

require 'securerandom'

algos = ['seq_louvain', 'ompRelaxmap', 'plm', 'infomap']

algos.each do |algo|
  `mkdir data/results/meltdown_cmp/#{algo}`
  `mkdir data/results/meltdown_cmp/#{algo}/clusterings`
end

["com-lj.ungraph", "com-orkut.ungraph", "uk-2002.metis"].each do |name|
  bin_graph_path = "/algoDaten/zeitz/graphs/#{name}-preprocessed-*.bin"

  algos.each do |algo|
    10.times do |i|
      seed = rand(2**31 - 1)

      if algo == 'seq_louvain' || algo == 'infomap'
        cmd = "./#{algo} -b '#{bin_graph_path}' -o data/results/meltdown_cmp/#{algo}/clusterings/#{name}.run#{i}.part.txt"
      elsif algo == 'ompRelaxmap'
        cmd = "./#{algo} #{seed} '#{bin_graph_path}' 16 1 1e-3 0.0 10 data/results/meltdown_cmp/#{algo}/clusterings/#{name}.run#{i}.part.bin prior"
      elsif algo == 'plm'
        cmd = "./#{algo} '#{bin_graph_path}' data/results/meltdown_cmp/#{algo}/clusterings/#{name}.run#{i}.part.txt"
      else
        cmd = "SEED=#{seed} ./#{algo} '#{bin_graph_path}' data/results/meltdown_cmp/#{algo}/clusterings/#{name}.run#{i}-@@@@-#####.bin,#{SecureRandom.uuid}"
      end
      puts cmd
      begin
        system "#{cmd} | tee data/results/meltdown_cmp/#{algo}/job_#{name}_#{i}.out"
      rescue Exception => _
      end
    end
  end
end
