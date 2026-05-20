require "./perf_spec_helper"

private def preload_consume_messages(ch : Amqp::Channel,
                                     queue : String,
                                     body : Bytes,
                                     count : Int32,
                                     batch_size : Int32) : Nil
  full_batch = Array.new(batch_size) { body }
  tail_batch = Array.new(count % batch_size) { body }
  remaining = count

  while remaining >= batch_size
    ch.publish_batch(full_batch, "", queue)
    remaining -= batch_size
  end
  ch.publish_batch(tail_batch, "", queue) unless tail_batch.empty?
end

private def drain_with_ack(sub : Amqp::Subscription, count : Int32, expected_body_size : Int32) : Nil
  count.times do
    delivery = sub.receive
    raise "bad consume payload size" unless delivery.body.size == expected_body_size
    delivery.ack
  end
end

describe "T-PERF-CONS-001" do
  it "measures preloaded manual-ack consume throughput over a live broker workload" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    message_count = PerfSpecHelper.int_env("AMQP_PERF_CONS_MESSAGES", 10_000)
    warmup_count = {PerfSpecHelper.int_env("AMQP_PERF_CONS_WARMUP_MESSAGES", 1_000), message_count}.min
    body_bytes = PerfSpecHelper.int_env("AMQP_PERF_BODY_BYTES", 256)
    batch_size = PerfSpecHelper.int_env("AMQP_PERF_CONS_BATCH_SIZE", 100)
    prefetch_count = PerfSpecHelper.int_env("AMQP_PERF_CONS_PREFETCH", 1_000)
    raise "AMQP_PERF_CONS_PREFETCH must fit UInt16" if prefetch_count > UInt16::MAX.to_i

    bound = PerfSpecHelper.float_env("AMQP_PERF_CONS_001_MIN", 1.0)
    body = Bytes.new(body_bytes, 120_u8)

    elapsed = Time::Span.zero
    buffer = 0
    sub = nil
    warmup_sub = nil

    Amqp.connect(SpecHelper.amqp_url, recovery: Amqp::Recovery::None) do |conn|
      ch = conn.channel
      queue = "amqp-ng-perf-cons-#{Process.pid}-#{Time.utc.to_unix_ms}"
      default_buffer = PerfSpecHelper.legal_subscription_buffer(message_count, conn.config)
      buffer = PerfSpecHelper.int_env("AMQP_PERF_CONS_BUFFER", default_buffer)

      ch.queue_declare(queue, exclusive: true)
      ch.qos(prefetch_count.to_u16)

      begin
        preload_consume_messages(ch, queue, body, warmup_count, batch_size)
        warmup_sub = ch.consume(queue, no_ack: false, buffer: buffer)
        drain_with_ack(warmup_sub, warmup_count, body.size)
        warmup_sub.close rescue nil
        ch.queue_purge(queue)

        preload_consume_messages(ch, queue, body, message_count, batch_size)
        sub = ch.consume(queue, no_ack: false, buffer: buffer)
        started = Time.instant
        drain_with_ack(sub, message_count, body.size)
        elapsed = Time.instant - started
        sub.close rescue nil
        ch.queue_purge(queue)
      ensure
        sub.try &.close rescue nil
        warmup_sub.try &.close rescue nil
        ch.queue_delete(queue) rescue nil
      end
    end

    rate = message_count.to_f64 / elapsed.total_seconds
    passed = rate >= bound
    metadata = {
      "message_count"  => JSON::Any.new(message_count.to_i64),
      "warmup_count"   => JSON::Any.new(warmup_count.to_i64),
      "body_bytes"     => JSON::Any.new(body_bytes.to_i64),
      "batch_size"     => JSON::Any.new(batch_size.to_i64),
      "prefetch_count" => JSON::Any.new(prefetch_count.to_i64),
      "consume_buffer" => JSON::Any.new(buffer.to_i64),
      "amqp_url"       => JSON::Any.new(SpecHelper.amqp_url),
    }
    PerfSpecHelper.write_result(
      "T-PERF-CONS-001", "consume_ack_preloaded", rate, bound, "msg/s", passed, metadata
    )

    rate.should be >= bound
  end
end
