#!/usr/bin/env ruby

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph'))

5.times do |i|
  graphs.each do |graph|
    name = graph.split('/').last.split('.').first
    ground_proof = "#{graph[0..-6]}part"
    puts '#' * 64, name, '#' * 64
    begin
      `./louvain #{graph} #{File.exist?(ground_proof) ? ground_proof : ''} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[1]} ../../../scripts/report_to_json.rb`
    rescue Exception => _
    end
    if File.exist?('core')
      `mv core #{"core-#{name}-#{i}"}`
    end
  end
end
