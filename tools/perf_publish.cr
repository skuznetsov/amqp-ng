require "json"
require "log"

require "../src/amqp"

Log.setup(:warn)

url = ENV["AMQP_BENCH_URL"]? || ENV["AMQP_URL"]? || "amqp://guest:guest@127.0.0.1:5672/"
publish_n = (ENV["AMQP_BENCH_PUBLISH_N"]? || "20000").to_i
confirm_n = (ENV["AMQP_BENCH_CONFIRM_N"]? || "3000").to_i
batch_size = (ENV["AMQP_BENCH_BATCH_SIZE"]? || "100").to_i
samples = (ENV["AMQP_BENCH_SAMPLES"]? || "5").to_i
body_bytes = (ENV["AMQP_BENCH_BODY_BYTES"]? || "256").to_i
stage_n = (ENV["AMQP_BENCH_STAGE_N"]? || {publish_n, 200_000}.max.to_s).to_i
channel_counts = (ENV["AMQP_BENCH_CHANNELS"]? || "1,2,4,8")
  .split(',')
  .map(&.strip)
  .reject(&.empty?)
  .map(&.to_i)
  .uniq
  .sort
connection_counts = (ENV["AMQP_BENCH_CONNECTIONS"]? || "1,2,4")
  .split(',')
  .map(&.strip)
  .reject(&.empty?)
  .map(&.to_i)
  .uniq
  .sort
confirm_windows = (ENV["AMQP_BENCH_CONFIRM_WINDOWS"]? || "1,10,50,100,500")
  .split(',')
  .map(&.strip)
  .reject(&.empty?)
  .map(&.to_i)
  .uniq
  .sort

raise "AMQP_BENCH_PUBLISH_N must be positive" unless publish_n > 0
raise "AMQP_BENCH_CONFIRM_N must be positive" unless confirm_n > 0
raise "AMQP_BENCH_BATCH_SIZE must be positive" unless batch_size > 0
raise "AMQP_BENCH_SAMPLES must be positive" unless samples > 0
raise "AMQP_BENCH_BODY_BYTES must be non-negative" unless body_bytes >= 0
raise "AMQP_BENCH_STAGE_N must be positive" unless stage_n > 0
raise "AMQP_BENCH_CHANNELS must contain at least one positive integer" unless channel_counts.any? { |n| n > 0 }
raise "AMQP_BENCH_CHANNELS must contain only positive integers" unless channel_counts.all? { |n| n > 0 }
raise "AMQP_BENCH_CONNECTIONS must contain at least one positive integer" unless connection_counts.any? { |n| n > 0 }
raise "AMQP_BENCH_CONNECTIONS must contain only positive integers" unless connection_counts.all? { |n| n > 0 }
raise "AMQP_BENCH_CONFIRM_WINDOWS must contain at least one positive integer" unless confirm_windows.any? { |n| n > 0 }
raise "AMQP_BENCH_CONFIRM_WINDOWS must contain only positive integers" unless confirm_windows.all? { |n| n > 0 }

def median(values : Array(Float64)) : Float64
  sorted = values.sort
  mid = sorted.size // 2
  if sorted.size.odd?
    sorted[mid]
  else
    (sorted[mid - 1] + sorted[mid]) / 2.0
  end
end

def redact_url(url : String) : String
  uri = URI.parse(url)
  authority = uri.host || ""
  authority += ":#{uri.port}" if uri.port
  path = uri.path.presence || "/"
  query = uri.query ? "?..." : ""
  "#{uri.scheme}://#{authority}#{path}#{query}"
rescue
  "<unparseable>"
end

def sample_rates(label : String, samples : Int32, count : Int32, & : ->) : Array(Float64)
  rates = [] of Float64
  samples.times do
    started = Time.instant
    yield
    elapsed = Time.instant - started
    raise "#{label}: benchmark sample elapsed time was zero; increase workload count" unless elapsed.total_nanoseconds > 0

    rates << (count.to_f64 / elapsed.total_seconds)
  end
  rates
end

def publish_concurrently(channels : Array(Amqp::Channel), count : Int32, message : Amqp::Message, queue : String) : Nil
  publish_concurrently(channels, count, message, Array.new(channels.size, queue))
end

def publish_concurrently(channels : Array(Amqp::Channel), count : Int32, message : Amqp::Message, queues : Array(String)) : Nil
  raise "channels and queues size mismatch" unless channels.size == queues.size

  done = ::Channel(Nil).new(channels.size)
  per_channel = count // channels.size
  remainder = count % channels.size
  channels.each_with_index do |pub_ch, index|
    channel_count = per_channel + (index < remainder ? 1 : 0)
    queue = queues[index]
    spawn do
      channel_count.times do
        pub_ch.publish(message, "", queue)
      end
      done.send(nil)
    end
  end
  channels.size.times { done.receive }
end

def write_empty_property_publish(io : IO, channel : UInt16, exchange : String, routing_key : String, body : Bytes) : Nil
  Amqp::Wire::AmqpZeroNineOne::BasicMethods.write_publish_frame(
    io, channel, exchange, routing_key, mandatory: false, immediate: false)
  Amqp::Wire::AmqpZeroNineOne::ContentHeader.write_empty_frame(
    io, channel, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC, body.size.to_u64,
  )
  unless body.empty?
    Amqp::Wire::Frame.write_prefix(io, Amqp::Wire::FrameType::Body, channel, body.size)
    io.write(body)
    io.write_byte(Amqp::Wire::FRAME_END)
  end
end

def parse_basic_ack_frame_generic(frame_bytes : Bytes, frame_max : UInt32) : {UInt64, Bool}
  io = IO::Memory.new(frame_bytes, false)
  frame = Amqp::Wire::Frame.read(io, frame_max)
  body = IO::Memory.new(frame.payload, false)
  class_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
  method_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
  unless class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC &&
         method_id == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_ACK
    raise "expected basic.ack"
  end
  ack = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Ack.read(body)
  {ack.delivery_tag, ack.multiple}
end

def read_u16_be(bytes : Bytes, offset : Int32) : UInt16
  ((bytes[offset].to_u16 << 8) | bytes[offset + 1].to_u16).to_u16
end

def read_u32_be(bytes : Bytes, offset : Int32) : UInt32
  ((bytes[offset].to_u32 << 24) |
    (bytes[offset + 1].to_u32 << 16) |
    (bytes[offset + 2].to_u32 << 8) |
    bytes[offset + 3].to_u32).to_u32
end

def read_u64_be(bytes : Bytes, offset : Int32) : UInt64
  value = 0_u64
  8.times do |i|
    value = (value << 8) | bytes[offset + i].to_u64
  end
  value
end

def parse_basic_ack_frame_direct(frame_bytes : Bytes, frame_max : UInt32) : {UInt64, Bool}
  raise "short frame" if frame_bytes.size < 21
  raise "expected method frame" unless frame_bytes[0] == Amqp::Wire::FrameType::Method.value

  length = read_u32_be(frame_bytes, 3)
  cap = frame_max == 0 ? 131_072_u32 : frame_max
  raise "frame too large" if length > cap - 8
  raise "unexpected basic.ack length #{length}" unless length == 13
  raise "bad frame end" unless frame_bytes[20] == Amqp::Wire::FRAME_END
  raise "expected basic class" unless read_u16_be(frame_bytes, 7) == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC
  raise "expected basic.ack" unless read_u16_be(frame_bytes, 9) == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_ACK

  {read_u64_be(frame_bytes, 11), (frame_bytes[19] & 0x01) != 0}
end

def decode_empty_header_generic(payload : Bytes) : UInt64
  decoded = Amqp::Wire::AmqpZeroNineOne::ContentHeader.decode(payload)
  raise "expected empty properties" unless decoded.properties.empty?
  decoded.body_size
end

def decode_empty_header_direct(payload : Bytes) : UInt64
  decoded = Amqp::Wire::AmqpZeroNineOne::ContentHeader.decode_empty(payload) ||
            raise "expected direct empty header decode"
  decoded.body_size
end

def build_deliver_payload(consumer_tag : String,
                          delivery_tag : UInt64,
                          redelivered : Bool,
                          exchange : String,
                          routing_key : String) : Bytes
  io = IO::Memory.new
  io.write_bytes(Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
  io.write_bytes(Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_DELIVER, IO::ByteFormat::NetworkEndian)
  Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(io, consumer_tag)
  io.write_bytes(delivery_tag, IO::ByteFormat::NetworkEndian)
  io.write_byte(redelivered ? 1_u8 : 0_u8)
  Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(io, exchange)
  Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(io, routing_key)
  io.to_slice
end

def parse_basic_deliver_generic(payload : Bytes) : UInt64
  io = IO::Memory.new(payload[4, payload.size - 4], false)
  deliver = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver.read(io)
  deliver.delivery_tag &+ deliver.routing_key.bytesize.to_u64
end

def parse_basic_deliver_direct(payload : Bytes) : UInt64
  deliver = Amqp::Wire::AmqpZeroNineOne::BasicMethods.decode_deliver_frame_payload(payload) ||
            raise "expected direct deliver decode"
  deliver.delivery_tag &+ deliver.routing_key.bytesize.to_u64
end

body = Bytes.new(body_bytes, 120_u8)
single_message = Amqp::Message.new(body)
full_batch = Array.new(batch_size) { Amqp::Message.new(body) }
tail_messages = Array.new(publish_n % batch_size) { Amqp::Message.new(body) }
full_batch_bodies = Array.new(batch_size) { body }
tail_bodies = Array.new(publish_n % batch_size) { body }

results = {} of String => Array(Float64)
stages = {} of String => Array(Float64)
queue = ""

stages["encode_empty_publish_frames"] = sample_rates("encode_empty_publish_frames", samples, publish_n) do
  publish_n.times do
    io = IO::Memory.new
    write_empty_property_publish(io, 1_u16, "", "bench", body)
  end
end

ack_payload = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Ack.new(42_u64, false).to_payload
ack_frame_io = IO::Memory.new
Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, 1_u16, ack_payload).write(ack_frame_io)
ack_frame = ack_frame_io.to_slice
{parse_basic_ack_frame_generic(ack_frame, 131_072_u32),
 parse_basic_ack_frame_direct(ack_frame, 131_072_u32)}.each do |tag, multiple|
  raise "bad ack parse" unless tag == 42_u64 && !multiple
end

stages["parse_basic_ack_frame_generic"] = sample_rates("parse_basic_ack_frame_generic", samples, stage_n) do
  stage_n.times do
    tag, multiple = parse_basic_ack_frame_generic(ack_frame, 131_072_u32)
    raise "bad ack parse" unless tag == 42_u64 && !multiple
  end
end

stages["parse_basic_ack_frame_direct"] = sample_rates("parse_basic_ack_frame_direct", samples, stage_n) do
  stage_n.times do
    tag, multiple = parse_basic_ack_frame_direct(ack_frame, 131_072_u32)
    raise "bad ack parse" unless tag == 42_u64 && !multiple
  end
end

empty_header_payload = Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
  Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
  body.size.to_u64,
  Amqp::Properties.new,
)
{decode_empty_header_generic(empty_header_payload),
 decode_empty_header_direct(empty_header_payload)}.each do |body_size|
  raise "bad empty header decode" unless body_size == body.size.to_u64
end
empty_header_payloads = Array.new(256) do |i|
  Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
    Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
    body.size.to_u64 + i.to_u64,
    Amqp::Properties.new,
  )
end

stages["decode_empty_header_generic"] = sample_rates("decode_empty_header_generic", samples, stage_n) do
  sum = 0_u64
  expected = 0_u64
  stage_n.times do |i|
    index = i & 255
    body_size = decode_empty_header_generic(empty_header_payloads[index])
    expected &+= body.size.to_u64 + index.to_u64
    sum &+= body_size
  end
  raise "bad empty header decode checksum" unless sum == expected
end

stages["decode_empty_header_direct"] = sample_rates("decode_empty_header_direct", samples, stage_n) do
  sum = 0_u64
  expected = 0_u64
  stage_n.times do |i|
    index = i & 255
    body_size = decode_empty_header_direct(empty_header_payloads[index])
    expected &+= body.size.to_u64 + index.to_u64
    sum &+= body_size
  end
  raise "bad empty header decode checksum" unless sum == expected
end

deliver_payloads = Array.new(256) do |i|
  build_deliver_payload("ctag-#{i}", i.to_u64 + 1_u64, i.odd?, "", "bench-q-#{i}")
end
{parse_basic_deliver_generic(deliver_payloads[42]),
 parse_basic_deliver_direct(deliver_payloads[42])}.each do |value|
  raise "bad deliver parse" unless value == 43_u64 + "bench-q-42".bytesize
end

stages["parse_basic_deliver_generic"] = sample_rates("parse_basic_deliver_generic", samples, stage_n) do
  sum = 0_u64
  expected = 0_u64
  stage_n.times do |i|
    index = i & 255
    expected &+= index.to_u64 + 1_u64 + "bench-q-#{index}".bytesize.to_u64
    sum &+= parse_basic_deliver_generic(deliver_payloads[index])
  end
  raise "bad deliver parse checksum" unless sum == expected
end

stages["parse_basic_deliver_direct"] = sample_rates("parse_basic_deliver_direct", samples, stage_n) do
  sum = 0_u64
  expected = 0_u64
  stage_n.times do |i|
    index = i & 255
    expected &+= index.to_u64 + 1_u64 + "bench-q-#{index}".bytesize.to_u64
    sum &+= parse_basic_deliver_direct(deliver_payloads[index])
  end
  raise "bad deliver parse checksum" unless sum == expected
end

Amqp.connect(url, recovery: Amqp::Recovery::None) do |conn|
  ch = conn.channel
  queue = ch.queue_declare("", exclusive: true, auto_delete: true).name

  ch.publish(single_message, "", queue)
  ch.queue_purge(queue)

  results["publish_single"] = sample_rates("publish_single", samples, publish_n) do
    publish_n.times do
      ch.publish(single_message, "", queue)
    end
    ch.queue_purge(queue)
  end

  results["publish_batch"] = sample_rates("publish_batch", samples, publish_n) do
    remaining = publish_n
    while remaining >= batch_size
      ch.publish_batch(full_batch, "", queue)
      remaining -= batch_size
    end
    ch.publish_batch(tail_messages, "", queue) unless tail_messages.empty?
    ch.queue_purge(queue)
  end

  results["publish_batch_bytes"] = sample_rates("publish_batch_bytes", samples, publish_n) do
    remaining = publish_n
    while remaining >= batch_size
      ch.publish_batch(full_batch_bodies, "", queue)
      remaining -= batch_size
    end
    ch.publish_batch(tail_bodies, "", queue) unless tail_bodies.empty?
    ch.queue_purge(queue)
  end

  channel_counts.each do |channel_count|
    channels = Array.new(channel_count) { conn.channel }

    results["publish_multi_channel_#{channel_count}"] = sample_rates("publish_multi_channel_#{channel_count}", samples, publish_n) do
      publish_concurrently(channels, publish_n, single_message, queue)
      ch.queue_purge(queue)
    end

    channels.each(&.close)
  end

  shared_queue = "amqp-ng-bench-#{Process.pid}-#{Time.utc.to_unix_ms}"
  ch.queue_declare(shared_queue)
  begin
    connection_counts.each do |connection_count|
      pub_conns = Array.new(connection_count) { Amqp.connect(url, recovery: Amqp::Recovery::None) }
      channels = pub_conns.map(&.channel)

      results["publish_multi_connection_#{connection_count}"] = sample_rates("publish_multi_connection_#{connection_count}", samples, publish_n) do
        publish_concurrently(channels, publish_n, single_message, shared_queue)
        ch.queue_purge(shared_queue)
      end

      separate_queues = channels.each_index.map do |index|
        name = "#{shared_queue}-q#{index}"
        ch.queue_declare(name)
        name
      end.to_a

      results["publish_multi_connection_separate_queues_#{connection_count}"] = sample_rates("publish_multi_connection_separate_queues_#{connection_count}", samples, publish_n) do
        publish_concurrently(channels, publish_n, single_message, separate_queues)
        separate_queues.each { |name| ch.queue_purge(name) }
      end

      separate_queues.each { |name| ch.queue_delete(name) rescue nil }
    ensure
      separate_queues.try &.each { |name| ch.queue_delete(name) rescue nil }
      channels.try &.each { |pub_ch| pub_ch.close rescue nil }
      pub_conns.try &.each { |pub_conn| pub_conn.close rescue nil }
    end
  ensure
    ch.queue_delete(shared_queue) rescue nil
  end

  consume_queue = "amqp-ng-bench-consume-#{Process.pid}-#{Time.utc.to_unix_ms}"
  ch.queue_declare(consume_queue, exclusive: true)
  begin
    results["consume_no_ack_preloaded"] = [] of Float64
    samples.times do
      remaining = publish_n
      while remaining >= batch_size
        ch.publish_batch(full_batch_bodies, "", consume_queue)
        remaining -= batch_size
      end
      ch.publish_batch(tail_bodies, "", consume_queue) unless tail_bodies.empty?

      started = Time.instant
      sub = ch.consume(consume_queue, no_ack: true, buffer: {publish_n, 8192}.min)
      publish_n.times do
        delivery = sub.receive
        raise "bad consume payload size" unless delivery.body.size == body.size
      end
      elapsed = Time.instant - started
      raise "consume_no_ack_preloaded: benchmark sample elapsed time was zero; increase workload count" unless elapsed.total_nanoseconds > 0

      results["consume_no_ack_preloaded"] << (publish_n.to_f64 / elapsed.total_seconds)
    ensure
      sub.try &.close rescue nil
      ch.queue_purge(consume_queue) rescue nil
    end
  ensure
    ch.queue_delete(consume_queue) rescue nil
  end

  consume_ack_queue = "amqp-ng-bench-consume-ack-#{Process.pid}-#{Time.utc.to_unix_ms}"
  ch.queue_declare(consume_ack_queue, exclusive: true)
  begin
    results["consume_ack_preloaded"] = [] of Float64
    samples.times do
      remaining = publish_n
      while remaining >= batch_size
        ch.publish_batch(full_batch_bodies, "", consume_ack_queue)
        remaining -= batch_size
      end
      ch.publish_batch(tail_bodies, "", consume_ack_queue) unless tail_bodies.empty?

      started = Time.instant
      sub = ch.consume(consume_ack_queue, no_ack: false, buffer: {publish_n, 8192}.min)
      publish_n.times do
        delivery = sub.receive
        raise "bad consume ack payload size" unless delivery.body.size == body.size
        delivery.ack
      end
      elapsed = Time.instant - started
      raise "consume_ack_preloaded: benchmark sample elapsed time was zero; increase workload count" unless elapsed.total_nanoseconds > 0

      results["consume_ack_preloaded"] << (publish_n.to_f64 / elapsed.total_seconds)
    ensure
      sub.try &.close rescue nil
      ch.queue_purge(consume_ack_queue) rescue nil
    end
  ensure
    ch.queue_delete(consume_ack_queue) rescue nil
  end

  confirm_ch = conn.channel
  confirm_queue = confirm_ch.queue_declare("", exclusive: true, auto_delete: true).name
  confirm_ch.confirm_select
  confirm_ch.publish_confirm(single_message, "", confirm_queue, timeout: 5.seconds)
  confirm_ch.queue_purge(confirm_queue)

  results["confirm_sync"] = sample_rates("confirm_sync", samples, confirm_n) do
    confirm_n.times do
      confirm_ch.publish_confirm(single_message, "", confirm_queue, timeout: 5.seconds)
    end
    confirm_ch.queue_purge(confirm_queue)
  end

  confirm_full_batch = Array.new(batch_size) { Amqp::Message.new(body) }
  confirm_tail_messages = Array.new(confirm_n % batch_size) { Amqp::Message.new(body) }

  results["confirm_batch_wait"] = sample_rates("confirm_batch_wait", samples, confirm_n) do
    remaining = confirm_n
    while remaining >= batch_size
      confirm_ch.publish_batch(confirm_full_batch, "", confirm_queue)
      remaining -= batch_size
    end
    confirm_ch.publish_batch(confirm_tail_messages, "", confirm_queue) unless confirm_tail_messages.empty?
    raise "wait_for_confirms timed out or saw nack" unless confirm_ch.wait_for_confirms(30.seconds)
    confirm_ch.queue_purge(confirm_queue)
  end

  confirm_full_batch_bodies = Array.new(batch_size) { body }
  confirm_tail_bodies = Array.new(confirm_n % batch_size) { body }

  results["confirm_batch_wait_bytes"] = sample_rates("confirm_batch_wait_bytes", samples, confirm_n) do
    remaining = confirm_n
    while remaining >= batch_size
      confirm_ch.publish_batch(confirm_full_batch_bodies, "", confirm_queue)
      remaining -= batch_size
    end
    confirm_ch.publish_batch(confirm_tail_bodies, "", confirm_queue) unless confirm_tail_bodies.empty?
    raise "wait_for_confirms timed out or saw nack" unless confirm_ch.wait_for_confirms(30.seconds)
    confirm_ch.queue_purge(confirm_queue)
  end

  default_window_messages = Array.new(confirm_n) { Amqp::Message.new(body) }
  default_window_bodies = Array.new(confirm_n) { body }

  results["confirm_window_default"] = sample_rates("confirm_window_default", samples, confirm_n) do
    ok = confirm_ch.publish_confirm_batch(
      default_window_messages,
      "",
      confirm_queue,
      timeout: 30.seconds,
    )
    raise "publish_confirm_batch default timed out or saw nack" unless ok
    confirm_ch.queue_purge(confirm_queue)
  end

  results["confirm_window_bytes_default"] = sample_rates("confirm_window_bytes_default", samples, confirm_n) do
    ok = confirm_ch.publish_confirm_batch(
      default_window_bodies,
      "",
      confirm_queue,
      timeout: 30.seconds,
    )
    raise "publish_confirm_batch bytes default timed out or saw nack" unless ok
    confirm_ch.queue_purge(confirm_queue)
  end

  confirm_windows.each do |window_size|
    window_messages = Array.new(confirm_n) { Amqp::Message.new(body) }
    window_bodies = Array.new(confirm_n) { body }

    results["confirm_window_#{window_size}"] = sample_rates("confirm_window_#{window_size}", samples, confirm_n) do
      ok = confirm_ch.publish_confirm_batch(
        window_messages,
        "",
        confirm_queue,
        window_size: window_size,
        timeout: 30.seconds,
      )
      raise "publish_confirm_batch timed out or saw nack" unless ok
      confirm_ch.queue_purge(confirm_queue)
    end

    results["confirm_window_bytes_#{window_size}"] = sample_rates("confirm_window_bytes_#{window_size}", samples, confirm_n) do
      ok = confirm_ch.publish_confirm_batch(
        window_bodies,
        "",
        confirm_queue,
        window_size: window_size,
        timeout: 30.seconds,
      )
      raise "publish_confirm_batch bytes timed out or saw nack" unless ok
      confirm_ch.queue_purge(confirm_queue)
    end
  end

  confirm_ch.queue_delete(confirm_queue) rescue nil
  ch.queue_delete(queue) rescue nil
end

JSON.build(STDOUT) do |json|
  json.object do
    json.field "tool", "amqp-ng publish microbench"
    json.field "url", redact_url(url)
    json.field "queue", queue
    json.field "publish_n", publish_n
    json.field "confirm_n", confirm_n
    json.field "batch_size", batch_size
    json.field "samples", samples
    json.field "body_bytes", body_bytes
    json.field "stage_n", stage_n
    json.field "channel_counts", channel_counts
    json.field "connection_counts", connection_counts
    json.field "confirm_windows", confirm_windows
    json.field "stages" do
      json.object do
        stages.keys.sort.each do |name|
          rates = stages[name]
          json.field name do
            json.object do
              json.field "unit", "ops/s"
              json.field "median", median(rates)
              json.field "samples", rates
            end
          end
        end
      end
    end
    json.field "metrics" do
      json.object do
        results.keys.sort.each do |name|
          rates = results[name]
          json.field name do
            json.object do
              json.field "unit", "msg/s"
              json.field "median", median(rates)
              json.field "samples", rates
            end
          end
        end
      end
    end
  end
end
STDOUT.puts
