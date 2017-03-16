#!/usr/bin/env ruby

require 'set'

def log(s)
  puts "#{'#' * 10} ANALYZE #{'#' * 10} #{s}"
end

NO_DISPOSE = ['Size', 'Sum', 'Gather', 'AllReduce']

undisposed = Set.new
current_stage_index = 0

ARGF.each_line do |line|
  if match = /\A\[host 0 worker 0 \d+\] (\w+)\(\)\s+stage (\w+)\.(\d+)/.match(line)
    action = match[1]
    operation = match[2]
    stage_index = Integer(match[3])

    case action
    when 'Execute'
      current_stage_index = stage_index
      undisposed << "#{operation}.#{stage_index}" unless NO_DISPOSE.include?(operation)
    when 'PushData'
      log("repushed #{operation}.#{stage_index}") if stage_index < current_stage_index && operation != 'Cache'
    when 'Dispose'
      undisposed.delete("#{operation}.#{stage_index}")
      log("late dispose of #{operation}.#{stage_index}") if stage_index < current_stage_index && operation != 'Cache'
    end
  end
  puts line
end

log("#{undisposed.size} undisposed stages")
undisposed.each do |stage|
  log("#{stage} was not disposed")
end
