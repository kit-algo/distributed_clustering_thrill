#!/usr/bin/env ruby

start = 0
prev = 0

ARGF.each_line do |line|
  line.strip!
  next unless line.start_with? '#LOG# '
  line = line[6..-1]

  key, value = line.split(':').map(&:strip)
  type, id, attribute = key.split('/')

  start = value.to_i if attribute == 'start_ts'
  if attribute.end_with? '_ts'
    puts "#{type}/#{id}/#{attribute}: #{(value.to_i - start) / 1000000.0}s since start, #{(value.to_i - prev) / 1000000.0}s since prev"
    prev = value.to_i
  end
end
