require "./perf_spec_helper"

private def total_allocated_bytes_after_gc : UInt64
  GC.collect
  GC.stats.total_bytes
end

private def positive_allocated_delta(after : UInt64, before : UInt64) : UInt64
  after > before ? after - before : 0_u64
end

describe "T-PERF-GC-001" do
  it "measures fire-and-forget publish allocation pressure" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    count = PerfSpecHelper.int_env("AMQP_PERF_GC_MESSAGES", 1_000_000)
    body_bytes = PerfSpecHelper.int_env("AMQP_PERF_BODY_BYTES", 256)
    bound = PerfSpecHelper.float_env("AMQP_PERF_GC_001_MAX_BYTES_PER_MESSAGE", 512.0)
    body = Bytes.new(body_bytes, 120_u8)
    message = Amqp::Message.new(body)

    before = 0_u64
    after = 0_u64
    delta = 0_u64

    Amqp.connect(SpecHelper.amqp_url, recovery: Amqp::Recovery::None) do |conn|
      ch = conn.channel
      queue = ch.queue_declare("", exclusive: true, auto_delete: true).name

      before = total_allocated_bytes_after_gc
      count.times do
        ch.publish(message, "", queue)
      end
      after = GC.stats.total_bytes
      delta = positive_allocated_delta(after, before)
      ch.queue_purge(queue)
    end

    bytes_per_message = delta.to_f64 / count
    passed = bytes_per_message <= bound
    metadata = {
      "message_count"      => JSON::Any.new(count.to_i64),
      "body_bytes"         => JSON::Any.new(body_bytes.to_i64),
      "total_bytes_before" => JSON::Any.new(before.to_i64),
      "total_bytes_after"  => JSON::Any.new(after.to_i64),
      "total_bytes_delta"  => JSON::Any.new(delta.to_i64),
      "gc_metric"          => JSON::Any.new("GC.stats.total_bytes"),
      "amqp_url"           => JSON::Any.new(PerfSpecHelper.redacted_url(SpecHelper.amqp_url)),
    }
    PerfSpecHelper.write_result(
      "T-PERF-GC-001", "publish_allocated_bytes", bytes_per_message, bound, "bytes/message", passed, metadata
    )

    bytes_per_message.should be <= bound
  end
end
