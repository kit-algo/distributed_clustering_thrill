#!/usr/bin/env ruby

require 'securerandom'
require 'json'

data = {}
local_id_to_uuid = Hash.new { |hash, key| hash[key] = SecureRandom.uuid }

def typify value
  case value
    when ->(it) { Integer(it) rescue false } then Integer(value)
    when ->(it) { Float(it) rescue false } then Float(value)
    else value
  end
end

ARGF.each_line do |line|
  next if line.strip.empty?

  key, value = line.split(':').map(&:strip)
  type, id, attribute = key.split('/')

  data[type] ||= {}
  data[type][local_id_to_uuid[id]] ||= {}
  if attribute.end_with? '_id'
    data[type][local_id_to_uuid[id]][attribute] = local_id_to_uuid[value]
  else
    data[type][local_id_to_uuid[id]][attribute] = typify value
  end
end

data.transform_values! { |type_items| type_items.map { |id, item| item.tap { item['id'] = id } } }

timestamp = Time.now
commit = `git rev-parse HEAD`.strip

data['program_run'].each do |run|
  run['timestamp'] = timestamp
  run['commit'] = commit
end


Dir.chdir ENV['MA_RESULT_OUTPUT_DIR'] if ENV['MA_RESULT_OUTPUT_DIR']
counter = 0
while File.exist?("report_#{counter}_#{timestamp.strftime "%Y-%m-%d"}_at_#{commit}.json")
  counter += 1
end
File.open("report_#{counter}_#{timestamp.strftime "%Y-%m-%d"}_at_#{commit}.json", 'w+') do |export|
  export.puts data.to_json
end
