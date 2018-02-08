#!/usr/bin/env ruby

require 'securerandom'
require 'json'
require 'pathname'

data = {}
local_id_to_uuid = Hash.new do |hash, key|
  hash[key] = if key =~ /\A\d+\Z/
    SecureRandom.uuid
  else
    key
  end
end

def typify value
  case value
    when "true" then true
    when "false" then false
    when /\A\[.*\]\Z/ then value[1..-2].split(',').map(&:strip).reject(&:empty?).map { |it| typify(it) }
    when ->(it) { Integer(it) rescue false } then Integer(value)
    when ->(it) { Float(it) rescue false } then Float(value)
    else value
  end
end

ARGF.each_line do |line|
  line.strip!
  next unless line.start_with? '#LOG# '
  line = line[6..-1]

  key, value = line.split(':').map(&:strip)
  type, id, attribute = key.split('/')

  data[type] ||= {}
  data[type][local_id_to_uuid[id]] ||= {}
  if attribute.end_with?('_id') && attribute != 'job_id'
    data[type][local_id_to_uuid[id]][attribute] = local_id_to_uuid[value]
  else
    data[type][local_id_to_uuid[id]][attribute] = typify value
  end
end

timestamp = Time.now
commit = ''
Dir.chdir(Pathname.new(__FILE__).realpath.dirname) do
  commit = `git rev-parse HEAD`.strip
end

if data['program_run']
  data['program_run'].each do |_id, run|
    run['timestamp'] = timestamp
    run['commit'] = commit
  end
end

exit if data.empty?

Dir.chdir ENV['MA_RESULT_OUTPUT_DIR'] if ENV['MA_RESULT_OUTPUT_DIR']

counter = 0
begin
  while File.exist?("report_#{timestamp.strftime "%Y-%m-%d"}_#{counter}_at_#{commit}.json")
    counter += 1
  end
  binding.irb
  File.open("report_#{timestamp.strftime "%Y-%m-%d"}_#{counter}_at_#{commit}.json", File::WRONLY|File::CREAT|File::EXCL) do |export|
    export.puts data.to_json
  end
  written = true
rescue Errno::EEXIST
  retry
end
