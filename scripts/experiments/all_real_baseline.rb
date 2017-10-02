#!/usr/bin/env ruby

graph_dir = '/algoDaten/zeitz/graphs/realworld'

graphs = [
  "#{graph_dir}/uk-2002.metis-preprocessed-*.bin",
  "#{graph_dir}/uk-2007-05.metis-preprocessed-*.bin",
  "#{graph_dir}/in-2004.metis-preprocessed-*.bin",
  "#{graph_dir}/com-friendster-preprocessed-*.bin",
  "#{graph_dir}/com-lj.ungraph-preprocessed-*.bin",
  "#{graph_dir}/com-orkut.ungraph-preprocessed-*.bin",
  "#{graph_dir}/com-youtube.ungraph-preprocessed-*.bin",
  "#{graph_dir}/com-amazon.ungraph-preprocessed-*.bin",
  "#{graph_dir}/europe.osm-preprocessed-*.bin",
]

`mkdir clusterings`

algos = ['seq_louvain', 'infomap']

graphs.each do |graph|
  algos.each do |algo|
    cmd = "./#{algo} -b #{graph} -o #{ARGV[1]}/clusterings/#{algo}-#{name}.part"
    puts cmd
    begin
      system "#{cmd} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[0]} ./report_to_json.rb"
    rescue Exception => _
    end
  end
end
