require "json"
require "./perf_compare_report"

if ARGV.size != 2
  STDERR.puts "usage: crystal run tools/perf_compare.cr -- <baseline.json> <current.json>"
  STDERR.puts "optional env:"
  STDERR.puts "  AMQP_BENCH_COMPARE_THRESHOLD_PCT=10"
  STDERR.puts "  AMQP_BENCH_COMPARE_FAIL_REGRESSION_PCT=15"
  exit 2
end

threshold_pct = (ENV["AMQP_BENCH_COMPARE_THRESHOLD_PCT"]? || "10").to_f64
fail_regression_pct = ENV["AMQP_BENCH_COMPARE_FAIL_REGRESSION_PCT"]?.try(&.to_f64)

baseline = JSON.parse(File.read(ARGV[0]))
current = JSON.parse(File.read(ARGV[1]))
deltas = AmqpPerfCompareReport.compare(baseline, current, threshold_pct)

AmqpPerfCompareReport.lines(deltas).each { |line| puts "- #{line}" }

failures = AmqpPerfCompareReport.regression_failures(deltas, fail_regression_pct)
unless failures.empty?
  STDERR.puts "perf comparison failed: #{failures.size} lane(s) regressed by at least #{fail_regression_pct}%"
  exit 1
end
