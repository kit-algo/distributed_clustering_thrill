#!/usr/bin/env ruby

require 'json'

graphs = Dir.glob(File.join(ARGV[0], '**/*.graph')) + Dir.glob(File.join(ARGV[0], '**/*.txt'))

json_files = Dir.glob(File.join(ARGV[1], '*.json'))
data = {}
json_files.each do |file|
  JSON.parse(File.read(file)).each do |item_key, items|
    if data[item_key]
      data[item_key].merge!(items)
    else
      data[item_key] = items
    end
  end
end


5.times do |i|
  graphs.each do |graph|
    name = graph.split('/').last.split('.').first
    ground_proof = "#{graph}.cmty"

    args = ''
    args << ' -g ' << ground_proof if File.exist?(ground_proof)
    args << ' -f ' if graph.end_with?('.txt')

    args << data['program_run'].select { |key, run| run['graph'] == graph }.map { |key, run| "#{run['output']},#{key}" }.join(" ")

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
