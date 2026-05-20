require "./perf_spec_helper"

describe "T-PERF-MULTICHAN-001" do
  it "measures same-connection multi-channel publish throughput over a live broker window" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    warmup = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_WARMUP_SECONDS", PerfSpecHelper::DEFAULT_WARMUP)
    )
    window = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_WINDOW_SECONDS", PerfSpecHelper::DEFAULT_WINDOW)
    )
    body_bytes = PerfSpecHelper.int_env("AMQP_PERF_BODY_BYTES", 256)
    channel_count = PerfSpecHelper.int_env("AMQP_PERF_MULTICHAN_COUNT", 8)
    bound = PerfSpecHelper.float_env("AMQP_PERF_MULTICHAN_001_MIN", 1.0)
    body = Bytes.new(body_bytes, 120_u8)
    message = Amqp::Message.new(body)

    count = 0_i64
    elapsed = Time::Span.zero

    Amqp.connect(SpecHelper.amqp_url, recovery: Amqp::Recovery::None) do |conn|
      admin = conn.channel
      queue = admin.queue_declare("", exclusive: true, auto_delete: true).name
      channels = Array.new(channel_count) { conn.channel }

      warmup_done = ::Channel(Nil).new(channel_count)
      warmup_deadline = Time.instant + warmup
      channels.each do |ch|
        spawn(name: "amqp-perf-multichan-warmup") do
          while Time.instant < warmup_deadline
            ch.publish(message, "", queue)
          end
          warmup_done.send(nil)
        end
      end
      channel_count.times { warmup_done.receive }
      admin.queue_purge(queue)

      done = ::Channel(Int64).new(channel_count)
      started = Time.instant
      deadline = started + window
      channels.each do |ch|
        spawn(name: "amqp-perf-multichan-publish") do
          local_count = 0_i64
          while Time.instant < deadline
            ch.publish(message, "", queue)
            local_count += 1
          end
          done.send(local_count)
        end
      end

      channel_count.times { count += done.receive }
      elapsed = Time.instant - started
      admin.queue_purge(queue)
    end

    rate = count.to_f64 / elapsed.total_seconds
    passed = rate >= bound
    metadata = {
      "warmup_seconds" => JSON::Any.new(warmup.total_seconds),
      "window_seconds" => JSON::Any.new(window.total_seconds),
      "body_bytes"     => JSON::Any.new(body_bytes.to_i64),
      "channel_count"  => JSON::Any.new(channel_count.to_i64),
      "amqp_url"       => JSON::Any.new(PerfSpecHelper.redacted_url(SpecHelper.amqp_url)),
    }
    PerfSpecHelper.write_result(
      "T-PERF-MULTICHAN-001", "publish_multi_channel", rate, bound, "msg/s", passed, metadata
    )

    rate.should be >= bound
  end
end
