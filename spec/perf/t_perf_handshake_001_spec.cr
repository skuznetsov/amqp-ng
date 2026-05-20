require "./perf_spec_helper"

describe "T-PERF-HANDSHAKE-001" do
  it "measures plain AMQP connect handshake p99 over sequential live broker connections" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    count = PerfSpecHelper.int_env("AMQP_PERF_HANDSHAKE_CONNECTIONS", 1_000)
    bound = PerfSpecHelper.float_env("AMQP_PERF_HANDSHAKE_P99_MS", 10.0)
    samples = PerfSpecHelper.measure_connect_ms(SpecHelper.amqp_url, count)
    p99 = PerfSpecHelper.percentile(samples, 0.99)
    passed = p99 <= bound
    metadata = {
      "connection_count" => JSON::Any.new(count.to_i64),
      "min_ms"           => JSON::Any.new(samples.min),
      "median_ms"        => JSON::Any.new(PerfSpecHelper.percentile(samples, 0.50)),
      "max_ms"           => JSON::Any.new(samples.max),
      "amqp_url"         => JSON::Any.new(PerfSpecHelper.redacted_url(SpecHelper.amqp_url)),
    }
    PerfSpecHelper.write_result(
      "T-PERF-HANDSHAKE-001", "handshake_plain_p99", p99, bound, "ms", passed, metadata
    )

    p99.should be <= bound
  end

  it "measures TLS AMQP connect handshake p99 over sequential live broker connections" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    tls_url = ENV["AMQP_TLS_URL"]?
    pending! "AMQP_TLS_URL not set" unless tls_url
    tls_context = PerfSpecHelper.tls_context_from_env
    pending! "TLS broker not reachable" unless PerfSpecHelper.reachable?(tls_url, tls_context)

    count = PerfSpecHelper.int_env("AMQP_PERF_TLS_HANDSHAKE_CONNECTIONS",
      PerfSpecHelper.int_env("AMQP_PERF_HANDSHAKE_CONNECTIONS", 1_000))
    bound = PerfSpecHelper.float_env("AMQP_PERF_TLS_HANDSHAKE_P99_MS", 50.0)
    samples = PerfSpecHelper.measure_connect_ms(tls_url, count, tls_context)
    p99 = PerfSpecHelper.percentile(samples, 0.99)
    passed = p99 <= bound
    metadata = {
      "connection_count" => JSON::Any.new(count.to_i64),
      "min_ms"           => JSON::Any.new(samples.min),
      "median_ms"        => JSON::Any.new(PerfSpecHelper.percentile(samples, 0.50)),
      "max_ms"           => JSON::Any.new(samples.max),
      "amqp_url"         => JSON::Any.new(PerfSpecHelper.redacted_url(tls_url)),
    }
    PerfSpecHelper.write_result(
      "T-PERF-HANDSHAKE-001-TLS", "handshake_tls_p99", p99, bound, "ms", passed, metadata
    )

    p99.should be <= bound
  end
end
