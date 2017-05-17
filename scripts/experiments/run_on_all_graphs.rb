#!/usr/bin/env ruby

require 'json'

system "./generate_external_partitions.rb #{ARGV[1..-1].map { |graph| "'#{graph}'" }.join(' ')} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[0]} ./report_to_json.rb"

json_files = Dir.glob(File.join(ARGV[0], '*.json'))
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


3.times do |i|
  ARGV[1..-1].each do |graph|
    name = graph.split('/').last.split('.').first
    ground_proof = "#{graph}.cmty"

    args = ' '
    args << '-g ' << ground_proof << ' ' if File.exist?(ground_proof)
    args << '-f ' if graph.end_with?('.txt')
    args << '-b ' if graph.end_with?('.bin')

    args << data['program_run'].select { |key, run| run['graph'] == graph }.map { |key, run| "#{run['output']},#{key}" }.join(" ")

    puts '#' * 64, name, '#' * 64
    puts "./louvain '#{graph}' #{args}"
    begin
      system "./louvain '#{graph}' #{args} | tee /dev/tty | MA_RESULT_OUTPUT_DIR=#{ARGV[0]} ./report_to_json.rb"
    rescue Exception => _
    end
    if File.exist?('core')
      `mv core #{"core-#{name}-#{i}"}`
    end
  end
end
