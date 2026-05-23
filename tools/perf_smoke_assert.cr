require "json"
require "./perf_smoke_assertions"

path = ARGV[0]? || "-"
raw = path == "-" ? STDIN.gets_to_end : File.read(path)
doc = JSON.parse(raw)

def env_float(name : String, fallback : Float64) : Float64
  value = ENV[name]?
  return fallback unless value
  parsed = value.to_f64?
  raise "#{name} must be a float" unless parsed
  parsed
end

def env_list(name : String, fallback : Array(String)) : Array(String)
  value = ENV[name]?
  return fallback unless value
  value
    .split(',')
    .map(&.strip)
    .reject(&.empty?)
end

errors = AmqpPerfSmokeAssert.validate(
  doc,
  min_metric_median: env_float("AMQP_BENCH_MIN_METRIC_MEDIAN", 1.0),
  min_stage_median: env_float("AMQP_BENCH_MIN_STAGE_MEDIAN", 1.0),
  required_metrics: env_list("AMQP_BENCH_REQUIRED_METRICS", AmqpPerfSmokeAssert::DEFAULT_REQUIRED_METRICS),
  required_stages: env_list("AMQP_BENCH_REQUIRED_STAGES", AmqpPerfSmokeAssert::DEFAULT_REQUIRED_STAGES),
)

unless errors.empty?
  STDERR.puts "perf smoke assertion failed:"
  errors.each { |error| STDERR.puts "- #{error}" }
  exit 1
end

puts "perf smoke assertion passed: #{doc["metrics"]?.try(&.as_h?.try(&.size)) || 0} metrics, #{doc["stages"]?.try(&.as_h?.try(&.size)) || 0} stages"
