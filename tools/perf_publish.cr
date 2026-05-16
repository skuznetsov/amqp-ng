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

raise "AMQP_BENCH_PUBLISH_N must be positive" unless publish_n > 0
raise "AMQP_BENCH_CONFIRM_N must be positive" unless confirm_n > 0
raise "AMQP_BENCH_BATCH_SIZE must be positive" unless batch_size > 0
raise "AMQP_BENCH_SAMPLES must be positive" unless samples > 0
raise "AMQP_BENCH_BODY_BYTES must be non-negative" unless body_bytes >= 0
raise "AMQP_BENCH_CHANNELS must contain at least one positive integer" unless channel_counts.any? { |n| n > 0 }
raise "AMQP_BENCH_CHANNELS must contain only positive integers" unless channel_counts.all? { |n| n > 0 }
raise "AMQP_BENCH_CONNECTIONS must contain at least one positive integer" unless connection_counts.any? { |n| n > 0 }
raise "AMQP_BENCH_CONNECTIONS must contain only positive integers" unless connection_counts.all? { |n| n > 0 }

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

def sample_rates(samples : Int32, count : Int32, & : ->) : Array(Float64)
  rates = [] of Float64
  samples.times do
    started = Time.instant
    yield
    elapsed = Time.instant - started
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

body = Bytes.new(body_bytes, 120_u8)
single_message = Amqp::Message.new(body)
full_batch = Array.new(batch_size) { Amqp::Message.new(body) }
tail_messages = Array.new(publish_n % batch_size) { Amqp::Message.new(body) }

results = {} of String => Array(Float64)
stages = {} of String => Array(Float64)
queue = ""

stages["encode_empty_publish_frames"] = sample_rates(samples, publish_n) do
  publish_n.times do
    io = IO::Memory.new
    write_empty_property_publish(io, 1_u16, "", "bench", body)
  end
end

Amqp.connect(url, recovery: Amqp::Recovery::None) do |conn|
  ch = conn.channel
  queue = ch.queue_declare("", exclusive: true, auto_delete: true).name

  ch.publish(single_message, "", queue)
  ch.queue_purge(queue)

  results["publish_single"] = sample_rates(samples, publish_n) do
    publish_n.times do
      ch.publish(single_message, "", queue)
    end
    ch.queue_purge(queue)
  end

  results["publish_batch"] = sample_rates(samples, publish_n) do
    remaining = publish_n
    while remaining >= batch_size
      ch.publish_batch(full_batch, "", queue)
      remaining -= batch_size
    end
    ch.publish_batch(tail_messages, "", queue) unless tail_messages.empty?
    ch.queue_purge(queue)
  end

  channel_counts.each do |channel_count|
    channels = Array.new(channel_count) { conn.channel }

    results["publish_multi_channel_#{channel_count}"] = sample_rates(samples, publish_n) do
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

      results["publish_multi_connection_#{connection_count}"] = sample_rates(samples, publish_n) do
        publish_concurrently(channels, publish_n, single_message, shared_queue)
        ch.queue_purge(shared_queue)
      end

      separate_queues = channels.each_index.map do |index|
        name = "#{shared_queue}-q#{index}"
        ch.queue_declare(name)
        name
      end.to_a

      results["publish_multi_connection_separate_queues_#{connection_count}"] = sample_rates(samples, publish_n) do
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

  confirm_ch = conn.channel
  confirm_queue = confirm_ch.queue_declare("", exclusive: true, auto_delete: true).name
  confirm_ch.confirm_select
  confirm_ch.publish_confirm(single_message, "", confirm_queue, timeout: 5.seconds)
  confirm_ch.queue_purge(confirm_queue)

  results["confirm_sync"] = sample_rates(samples, confirm_n) do
    confirm_n.times do
      confirm_ch.publish_confirm(single_message, "", confirm_queue, timeout: 5.seconds)
    end
    confirm_ch.queue_purge(confirm_queue)
  end

  confirm_full_batch = Array.new(batch_size) { Amqp::Message.new(body) }
  confirm_tail_messages = Array.new(confirm_n % batch_size) { Amqp::Message.new(body) }

  results["confirm_batch_wait"] = sample_rates(samples, confirm_n) do
    remaining = confirm_n
    while remaining >= batch_size
      confirm_ch.publish_batch(confirm_full_batch, "", confirm_queue)
      remaining -= batch_size
    end
    confirm_ch.publish_batch(confirm_tail_messages, "", confirm_queue) unless confirm_tail_messages.empty?
    raise "wait_for_confirms timed out or saw nack" unless confirm_ch.wait_for_confirms(30.seconds)
    confirm_ch.queue_purge(confirm_queue)
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
    json.field "channel_counts", channel_counts
    json.field "connection_counts", connection_counts
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
