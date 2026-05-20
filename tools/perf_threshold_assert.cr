require "json"
require "./perf_thresholds"

if ARGV.size != 2
  STDERR.puts "usage: crystal run tools/perf_threshold_assert.cr -- <benchmark.json> <thresholds.json>"
  exit 2
end

benchmark = JSON.parse(File.read(ARGV[0]))
thresholds = JSON.parse(File.read(ARGV[1]))
errors = AmqpPerfThresholds.validate(benchmark, thresholds)

unless errors.empty?
  profile = thresholds["profile"]?.try(&.as_s?) || "<unnamed>"
  STDERR.puts "perf threshold assertion failed for #{profile}:"
  errors.each { |error| STDERR.puts "- #{error}" }
  exit 1
end

profile = thresholds["profile"]?.try(&.as_s?) || "<unnamed>"
puts "perf threshold assertion passed for #{profile}"
