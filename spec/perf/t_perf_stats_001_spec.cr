require "./perf_spec_helper"

describe "T-PERF-STATS-001" do
  it "measures Stats#snapshot read overhead in a tight local loop" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?

    iterations = PerfSpecHelper.int_env("AMQP_PERF_STATS_READS", 1_000_000)
    bound = PerfSpecHelper.float_env("AMQP_PERF_STATS_001_MAX_US", 1.0)
    stats = Amqp::Stats.new
    sink = 0_i64

    started = Time.instant
    iterations.times do
      snap = stats.snapshot
      sink &+= snap.published
      sink &+= snap.confirmed_ack
      sink &+= snap.confirmed_nack
      sink &+= snap.returned
      sink &+= snap.consumed
      sink &+= snap.recoveries_attempted
      sink &+= snap.recoveries_succeeded
      sink &+= snap.recoveries_failed
    end
    elapsed = Time.instant - started
    raise "stats snapshot benchmark elapsed time was zero; increase read count" unless elapsed.total_nanoseconds > 0

    avg_us = elapsed.total_nanoseconds.to_f64 / iterations / 1_000.0
    passed = avg_us <= bound
    metadata = {
      "iterations"      => JSON::Any.new(iterations.to_i64),
      "elapsed_seconds" => JSON::Any.new(elapsed.total_seconds),
      "sink"            => JSON::Any.new(sink),
      "counter_count"   => JSON::Any.new(8_i64),
      "snapshot_object" => JSON::Any.new("Amqp::Stats::Snapshot"),
      "broker_required" => JSON::Any.new(false),
    }
    PerfSpecHelper.write_result(
      "T-PERF-STATS-001", "stats_snapshot_avg", avg_us, bound, "us/call", passed, metadata
    )

    avg_us.should be <= bound
  end
end
