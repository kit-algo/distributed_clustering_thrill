#!/usr/bin/env ruby

graph_dir = '/algoDaten/zeitz/graphs/realworld-preprocessed'

graphs = [
  "#{graph_dir}/com-amazon.ungraph-preprocessed-*.bin",
  "#{graph_dir}/com-youtube.ungraph-preprocessed-*.bin",
  "#{graph_dir}/in-2004.metis-preprocessed-*.bin",
  "#{graph_dir}/com-lj.ungraph-preprocessed-*.bin",
  "#{graph_dir}/europe.osm-preprocessed-*.bin",
  "#{graph_dir}/com-orkut.ungraph-preprocessed-*.bin",
  "#{graph_dir}/uk-2002.metis-preprocessed-*.bin",
  "#{graph_dir}/com-friendster-preprocessed-*.bin",
  "#{graph_dir}/uk-2007-05.metis-preprocessed-*.bin",
]

`mkdir #{ARGV[0]}/clusterings`

algos = ['seq_louvain', 'infomap']

graphs.each do |graph|
  name = graph.split('/').last.split('.')[0..-2].join('.')

  algos.each do |algo|
    cmd = "./#{algo} -b '#{graph}' -o #{ARGV[0]}/clusterings/#{algo}-#{name[0..-3]}.part"
    puts cmd
    begin
      system "#{cmd} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[0]} ./report_to_json.rb"
    rescue Exception => _
    end
  end
end
