#!/usr/bin/env ruby

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph')) + Dir.glob(File.join(ARGV[0], '**/*.txt'))

5.times do |i|
  graphs.each do |graph|
    name = graph.split('/').last.split('.').first
    ground_proof = "#{graph}.cmty"

    args = ''
    args << ' -g ' << ground_proof if File.exist?(ground_proof)
    args << ' -f ' if graph.end_with?('.txt')

    puts '#' * 64, name, '#' * 64
    begin
      `./louvain #{graph} #{args} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[1]} ../../../scripts/report_to_json.rb`
    rescue Exception => _
    end
    if File.exist?('core')
      `mv core #{"core-#{name}-#{i}"}`
    end
  end
end
