#!/usr/bin/env ruby

unless File.exists? ARGV[0]
  puts 'File not found! Exiting!'
  exit
end

File.open("#{ARGV[0]}.txt", 'w+') do |export|
  iterator = File.open(ARGV[0]).each
  n, m = iterator.next.split
  export.puts "# Nodes: #{n} Edges: #{m}"

  node_id = 1
  iterator.each do |line|
    line.split.map(&:to_i).each do |neighbor|
      export.puts "#{node_id} #{neighbor}" if node_id < neighbor
    end

    node_id += 1
  end
end

puts 'done - bye'
