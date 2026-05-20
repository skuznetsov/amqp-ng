require "./perf_spec_helper"

describe "T-PERF-PUB-001" do
  it "measures fire-and-forget publish throughput over a live broker window" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    warmup = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_WARMUP_SECONDS", PerfSpecHelper::DEFAULT_WARMUP)
    )
    window = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_WINDOW_SECONDS", PerfSpecHelper::DEFAULT_WINDOW)
    )
    body_bytes = PerfSpecHelper.int_env("AMQP_PERF_BODY_BYTES", 256)
    bound = PerfSpecHelper.float_env("AMQP_PERF_PUB_001_MIN", 1.0)
    body = Bytes.new(body_bytes, 120_u8)
    message = Amqp::Message.new(body)

    count = 0
    elapsed = Time::Span.zero

    Amqp.connect(SpecHelper.amqp_url, recovery: Amqp::Recovery::None) do |conn|
      ch = conn.channel
      queue = ch.queue_declare("", exclusive: true, auto_delete: true).name

      warmup_deadline = Time.instant + warmup
      while Time.instant < warmup_deadline
        ch.publish(message, "", queue)
      end
      ch.queue_purge(queue)

      started = Time.instant
      deadline = started + window
      while Time.instant < deadline
        ch.publish(message, "", queue)
        count += 1
      end
      elapsed = Time.instant - started
      ch.queue_purge(queue)
    end

    rate = count.to_f64 / elapsed.total_seconds
    passed = rate >= bound
    metadata = {
      "warmup_seconds" => JSON::Any.new(warmup.total_seconds),
      "window_seconds" => JSON::Any.new(window.total_seconds),
      "body_bytes"     => JSON::Any.new(body_bytes.to_i64),
      "amqp_url"       => JSON::Any.new(SpecHelper.amqp_url),
    }
    PerfSpecHelper.write_result(
      "T-PERF-PUB-001", "publish_single", rate, bound, "msg/s", passed, metadata
    )

    rate.should be >= bound
  end
end
