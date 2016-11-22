#!/usr/bin/env ruby

unless File.exists? ARGV[0]
  puts 'File not found! Exiting!'
  exit
end


n_declared = m_declared = m_counted = 0
adjacency_arrays = Hash.new { |hash, key| hash[key] = [] }

puts 'reading file'

File.open(ARGV[0]).each do |line|
  case line
  when /\A# Nodes: (\d+) Edges: (\d+)/  # Nodes: 317080 Edges: 1049866
    n_declared = $1.to_i
    m_declared = $2.to_i
  when /\A#/
  when /\A(\d+)\s+(\d+)\Z/
    adjacency_arrays[$1.to_i] << $2.to_i
    adjacency_arrays[$2.to_i] << $1.to_i
    m_counted += 1
  else
    puts "broken line #{line}"
  end
end

puts "actually found #{m_counted} edges rather than the declared #{m_declared}" if m_counted != m_declared

sorted_ids = adjacency_arrays.keys.sort
id_mapping = Hash[sorted_ids.zip(1..n_declared)]

puts 'exporting'

File.open("#{ARGV[0]}.graph", 'w+') do |export|
  export.puts "#{n_declared} #{m_counted}"

  sorted_ids.each do |id|
    export.puts adjacency_arrays[id].map { |adj_id| id_mapping[adj_id] }.sort.uniq.join(' ')
  end
end

puts 'done - bye'
