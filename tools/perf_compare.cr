require "json"
require "./perf_compare_report"

def env_bool(name : String) : Bool
  value = ENV[name]?
  return false unless value

  case value.downcase
  when "1", "true", "yes", "on"
    true
  when "0", "false", "no", "off"
    false
  else
    raise "#{name} must be one of 1/0, true/false, yes/no, or on/off"
  end
end

if ARGV.size != 2
  STDERR.puts "usage: crystal run tools/perf_compare.cr -- <baseline.json> <current.json>"
  STDERR.puts "optional env:"
  STDERR.puts "  AMQP_BENCH_COMPARE_THRESHOLD_PCT=10"
  STDERR.puts "  AMQP_BENCH_COMPARE_FAIL_REGRESSION_PCT=15"
  STDERR.puts "  AMQP_BENCH_COMPARE_FAIL_METADATA=1"
  STDERR.puts "  AMQP_BENCH_COMPARE_FAIL_MISSING_CURRENT=1"
  STDERR.puts "  AMQP_BENCH_COMPARE_FAIL_NEW_LANE=1"
  exit 2
end

threshold_pct = (ENV["AMQP_BENCH_COMPARE_THRESHOLD_PCT"]? || "10").to_f64
fail_regression_pct = ENV["AMQP_BENCH_COMPARE_FAIL_REGRESSION_PCT"]?.try(&.to_f64)
fail_metadata = env_bool("AMQP_BENCH_COMPARE_FAIL_METADATA")
fail_missing_current = env_bool("AMQP_BENCH_COMPARE_FAIL_MISSING_CURRENT")
fail_new_lane = env_bool("AMQP_BENCH_COMPARE_FAIL_NEW_LANE")

baseline = JSON.parse(File.read(ARGV[0]))
current = JSON.parse(File.read(ARGV[1]))
deltas = AmqpPerfCompareReport.compare(baseline, current, threshold_pct)
metadata_warnings = AmqpPerfCompareReport.metadata_warnings(baseline, current)

metadata_warnings.each do |warning|
  STDERR.puts "warning: #{warning}"
end

AmqpPerfCompareReport.lines(deltas).each { |line| puts "- #{line}" }

failed = false
failures = AmqpPerfCompareReport.regression_failures(deltas, fail_regression_pct)
unless failures.empty?
  STDERR.puts "perf comparison failed: #{failures.size} lane(s) regressed by at least #{fail_regression_pct}%"
  failed = true
end

metadata_failures = AmqpPerfCompareReport.metadata_failures(metadata_warnings, fail_metadata)
unless metadata_failures.empty?
  STDERR.puts "perf comparison failed: #{metadata_failures.size} metadata warning(s) and AMQP_BENCH_COMPARE_FAIL_METADATA=1"
  failed = true
end

missing_failures = AmqpPerfCompareReport.missing_current_failures(deltas, fail_missing_current)
unless missing_failures.empty?
  STDERR.puts "perf comparison failed: #{missing_failures.size} baseline lane(s) missing from current and AMQP_BENCH_COMPARE_FAIL_MISSING_CURRENT=1"
  failed = true
end

new_lane_failures = AmqpPerfCompareReport.new_lane_failures(deltas, fail_new_lane)
unless new_lane_failures.empty?
  STDERR.puts "perf comparison failed: #{new_lane_failures.size} new current lane(s) and AMQP_BENCH_COMPARE_FAIL_NEW_LANE=1"
  failed = true
end

if failed
  exit 1
end
