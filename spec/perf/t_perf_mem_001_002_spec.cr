require "./perf_spec_helper"

private def heap_size_after_gc : UInt64
  GC.collect
  GC.stats.heap_size
end

private def positive_heap_delta(after : UInt64, before : UInt64) : UInt64
  after > before ? after - before : 0_u64
end

private def wait_for_queue_depth(ch : Amqp::Channel,
                                 queue : String,
                                 max_messages : UInt32,
                                 timeout : Time::Span) : Nil
  deadline = Time.instant + timeout
  loop do
    info = ch.queue_declare(queue, passive: true)
    return if info.message_count <= max_messages

    raise "queue #{queue} did not drain to #{max_messages} messages" if Time.instant > deadline
    sleep 10.milliseconds
  end
end

describe "T-PERF-MEM-001" do
  it "measures retained heap per idle connection with one open channel" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    count = PerfSpecHelper.int_env("AMQP_PERF_MEM_CONNECTIONS", 100)
    bound = PerfSpecHelper.float_env("AMQP_PERF_MEM_001_MAX_BYTES", 64.0 * 1024.0)
    connections = [] of Amqp::Connection
    channels = [] of Amqp::Channel

    before = heap_size_after_gc
    count.times do
      conn = Amqp.connect(SpecHelper.amqp_url, recovery: Amqp::Recovery::None)
      connections << conn
      channels << conn.channel
    end
    after = heap_size_after_gc
    delta = positive_heap_delta(after, before)
    bytes_per_connection = delta.to_f64 / count
    passed = bytes_per_connection <= bound
    metadata = {
      "connection_count" => JSON::Any.new(count.to_i64),
      "heap_before"      => JSON::Any.new(before.to_i64),
      "heap_after"       => JSON::Any.new(after.to_i64),
      "heap_delta"       => JSON::Any.new(delta.to_i64),
      "amqp_url"         => JSON::Any.new(PerfSpecHelper.redacted_url(SpecHelper.amqp_url)),
    }
    PerfSpecHelper.write_result(
      "T-PERF-MEM-001", "idle_connection_heap", bytes_per_connection, bound, "bytes/connection", passed, metadata
    )

    bytes_per_connection.should be <= bound
  ensure
    channels.each { |ch| ch.close rescue nil } if channels
    connections.each { |conn| conn.close rescue nil } if connections
  end
end

describe "T-PERF-MEM-002" do
  it "measures retained heap per buffered unread delivery" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    message_count = PerfSpecHelper.int_env("AMQP_PERF_MEM_DELIVERIES", 100)
    body_bytes = PerfSpecHelper.int_env("AMQP_PERF_BODY_BYTES", 256)
    bound = PerfSpecHelper.float_env("AMQP_PERF_MEM_002_MAX_BYTES", 640.0)
    body = Bytes.new(body_bytes, 120_u8)
    sub = nil

    bytes_per_delivery = 0.0
    before = 0_u64
    after = 0_u64
    delta = 0_u64
    queue = ""

    Amqp.connect(SpecHelper.amqp_url, recovery: Amqp::Recovery::None) do |conn|
      consume_ch = conn.channel
      publish_ch = conn.channel
      queue = "amqp-ng-perf-mem-#{Process.pid}-#{Time.utc.to_unix_ms}"
      publish_ch.queue_declare(queue, exclusive: true)

      begin
        before = heap_size_after_gc
        sub = consume_ch.consume(queue, no_ack: true, buffer: message_count)
        messages = Array.new(message_count) { body }
        publish_ch.publish_batch(messages, "", queue)
        wait_for_queue_depth(publish_ch, queue, 0_u32, 5.seconds)

        after = heap_size_after_gc
        delta = positive_heap_delta(after, before)
        bytes_per_delivery = delta.to_f64 / message_count
      ensure
        sub.try &.close rescue nil
        publish_ch.queue_delete(queue) rescue nil
      end
    end

    passed = bytes_per_delivery <= bound
    metadata = {
      "delivery_count" => JSON::Any.new(message_count.to_i64),
      "body_bytes"     => JSON::Any.new(body_bytes.to_i64),
      "heap_before"    => JSON::Any.new(before.to_i64),
      "heap_after"     => JSON::Any.new(after.to_i64),
      "heap_delta"     => JSON::Any.new(delta.to_i64),
      "amqp_url"       => JSON::Any.new(PerfSpecHelper.redacted_url(SpecHelper.amqp_url)),
    }
    PerfSpecHelper.write_result(
      "T-PERF-MEM-002", "buffered_delivery_heap", bytes_per_delivery, bound, "bytes/delivery", passed, metadata
    )

    bytes_per_delivery.should be <= bound
  end
end
