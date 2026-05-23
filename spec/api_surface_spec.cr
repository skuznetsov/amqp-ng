require "./spec_helper"
require "../src/amqp-client"

describe "documented public API surface" do
  it "keeps the top-level Amqp namespace intentional" do
    constants = {{ Amqp.constants.map(&.stringify).sort }}
    constants.should eq([
      "Arguments",
      "AuthenticationError",
      "Channel",
      "ChannelClosedByBroker",
      "ChannelClosedByCaller",
      "ChannelError",
      "ChannelLimitError",
      "ChannelRpcTimeoutError",
      "ConcurrencyError",
      "Config",
      "ConfigurationError",
      "ConfirmOutcome",
      "ConnectError",
      "ConnectRefusedError",
      "ConnectTimeoutError",
      "Connection",
      "ConnectionClosedByBroker",
      "ConnectionClosedByCaller",
      "ConnectionError",
      "DeliverMessage",
      "Delivery",
      "Error",
      "Exchange",
      "FieldValue",
      "FrameTooLargeError",
      "GetMessage",
      "HeartbeatTimeoutError",
      "Message",
      "Persistence",
      "PreconditionFailedError",
      "PreparedPublisher",
      "Properties",
      "ProtocolError",
      "ProtocolNegotiationError",
      "PublishNackError",
      "PublishOutOfOrderError",
      "PublishReturnedError",
      "PublishTimeoutError",
      "Queue",
      "QueueDeclareOk",
      "QueueInfo",
      "Recovery",
      "RecoveryExhaustedError",
      "RecoveryInProgress",
      "ReturnReason",
      "ReturnedMessage",
      "SocketError",
      "Stats",
      "Subscription",
      "SubscriptionClosed",
      "TlsConfigError",
      "TlsHandshakeError",
      "UriError",
      "VERSION",
      "VhostAccessError",
      "Wire",
    ])
  end

  it "type-checks connection helpers and value types" do
    typeof(Amqp.connect(URI.parse("amqp://guest:guest@127.0.0.1/"))).should eq(Amqp::Connection)
    typeof(Amqp::Message.new("body")).should eq(Amqp::Message)
    typeof(Amqp::Recovery::Full).should eq(Amqp::Recovery)
    typeof(Amqp::Persistence::Persistent).should eq(Amqp::Properties::Persistence)
  end

  it "type-checks documented connection methods" do
    conn = uninitialized Amqp::Connection
    typeof(conn.closed?).should eq(Bool)
    typeof(conn.close_reason).to_s.should eq("(Exception | Nil)")
    typeof(conn.channel).should eq(Amqp::Channel)
    typeof(conn.channel(1_u16)).should eq(Amqp::Channel)
    typeof(conn.heartbeat).should eq(Time::Span)
    typeof(conn.channel_max).should eq(UInt16)
    typeof(conn.frame_max).should eq(UInt32)
    typeof(conn.server_properties).to_s.should eq("Hash(String, Amqp::FieldValue)")
    typeof(conn.stats).should eq(Amqp::Stats)
    typeof(conn.recovery_mode).should eq(Amqp::Recovery)
    typeof(conn.blocked?).should eq(Bool)
    typeof(conn.on_blocked { |reason| reason.size; nil }).should eq(Nil)
    typeof(conn.on_unblocked { nil }).should eq(Nil)
  end

  it "type-checks documented channel methods" do
    ch = uninitialized Amqp::Channel
    typeof(ch.open?).should eq(Bool)
    typeof(ch.confirms_enabled?).should eq(Bool)
    typeof(ch.prefetch(1_u16)).should eq(Nil)
    typeof(ch.publish(Amqp::Message.new("x"), "", "rk")).should eq(UInt64?)
    typeof(ch.publish_batch([Amqp::Message.new("x")], "", "rk")).should eq(Array(UInt64?))
    typeof(ch.publish_batch(["x".to_slice], "", "rk")).should eq(Array(UInt64?))
    typeof(ch.publish_confirm_batch([Amqp::Message.new("x")], "", "rk")).should eq(Bool)
    typeof(ch.publish_confirm_batch(["x".to_slice], "", "rk")).should eq(Bool)
    typeof(ch.prepared_publisher("", "rk")).should eq(Amqp::PreparedPublisher)
    typeof(ch.publish_confirm(Amqp::Message.new("x"), "", "rk")).should eq(Bool)
    typeof(ch.publish_async(Amqp::Message.new("x"), "", "rk")).should eq(Tuple(UInt64, ::Channel(Amqp::ConfirmOutcome)))
    typeof(ch.publish_async("x".to_slice, "", "rk")).should eq(Tuple(UInt64, ::Channel(Amqp::ConfirmOutcome)))
    typeof(ch.subscribe("q")).should eq(Amqp::Subscription)
    typeof(ch.get("q")).should eq(Amqp::GetMessage?)
    typeof(ch.queue_purge("q")).should eq(UInt32)
    typeof(ch.queue_unbind("q", "ex", "rk")).should eq(Nil)
    typeof(ch.exchange_delete("ex")).should eq(Nil)
    typeof(ch.exchange_bind("dest", "src", "rk")).should eq(Nil)
    typeof(ch.exchange_bind("dest", "src", "rk", no_wait: true)).should eq(Nil)
    typeof(ch.exchange_unbind("dest", "src", "rk")).should eq(Nil)
    typeof(ch.exchange_unbind("dest", "src", "rk", no_wait: true)).should eq(Nil)
    typeof(ch.queue_declare("q", arguments: {ttl: 1000_i32})).should eq(Amqp::QueueInfo)
    typeof(ch.queue_declare("q", no_wait: true)).should eq(Amqp::QueueInfo)
    typeof(ch.queue_bind("q", "ex", "rk", {priority: 1_u8})).should eq(Nil)
    typeof(ch.queue_bind("q", "ex", "rk", no_wait: true)).should eq(Nil)
    typeof(ch.queue_unbind("q", "ex", "rk", {priority: 1_u8})).should eq(Nil)
    typeof(ch.exchange_declare("ex", arguments: {alternate_exchange: "ae"})).should eq(Nil)
    typeof(ch.exchange_bind("dest", "src", "rk", {level: 1_i16})).should eq(Nil)
    typeof(ch.exchange_unbind("dest", "src", "rk", {level: 1_i16})).should eq(Nil)
    typeof(ch.basic_publish("x", "", "rk")).should eq(UInt64)
    typeof(ch.basic_publish(IO::Memory.new("x"), 1, "", "rk")).should eq(UInt64)
    typeof(ch.basic_publish(IO::Memory.new("x"), 1, "", "rk") { |ok| ok.to_s; nil }).should eq(UInt64)
    typeof(ch.basic_publish_confirm("x", "", "rk")).should eq(Bool)
    typeof(ch.basic_publish_confirm(IO::Memory.new("x"), 1, "", "rk")).should eq(Bool)
    typeof(ch.basic_get("q")).should eq(Amqp::GetMessage?)
    typeof(ch.basic_consume("q") { |msg| msg.ack }).should eq(String)
    typeof(ch.basic_consume("q", args: {prefetch_hint: 1_i32}) { |msg| msg.ack }).should eq(String)
    typeof(ch.basic_cancel("ctag")).should eq(Nil)
    typeof(ch.basic_ack(1_u64)).should eq(Nil)
    typeof(ch.basic_reject(1_u64)).should eq(Nil)
    typeof(ch.basic_nack(1_u64)).should eq(Nil)
    typeof(ch.basic_qos(1_u16)).should eq(Nil)
    typeof(ch.basic_recover).should eq(Nil)
    typeof(ch.flow(true)).should eq(Nil)
    typeof(ch.tx_select).should eq(Nil)
    typeof(ch.tx_commit).should eq(Nil)
    typeof(ch.tx_rollback).should eq(Nil)
    typeof(ch.transaction { 1 }).should eq(Int32)
    typeof(ch.on_return { |msg| msg.reason }).should eq(Nil)
    typeof(ch.on_cancel { |tag| tag.size; nil }).should eq(Nil)
    typeof(ch.on_close { |code, text| code.to_s + text; nil }).should eq(Nil)
    typeof(ch.queue).should eq(Amqp::Queue)
    typeof(ch.queue("q")).should eq(Amqp::Queue)
    typeof(ch.queue("q", args: {ttl: 1000_i32})).should eq(Amqp::Queue)
    typeof(ch.exchange("ex", "direct")).should eq(Amqp::Exchange)
    typeof(ch.exchange("ex", "direct", args: {alternate_exchange: "ae"})).should eq(Amqp::Exchange)
    typeof(ch.default_exchange).should eq(Amqp::Exchange)
    typeof(ch.direct_exchange).should eq(Amqp::Exchange)
    typeof(ch.topic_exchange).should eq(Amqp::Exchange)
    typeof(ch.fanout_exchange).should eq(Amqp::Exchange)
    typeof(ch.header_exchange).should eq(Amqp::Exchange)
  end

  it "type-checks queue and exchange wrappers" do
    queue = uninitialized Amqp::Queue
    exchange = uninitialized Amqp::Exchange
    typeof(queue.name).should eq(String)
    typeof(queue.bind("ex", "rk")).should eq(Amqp::Queue)
    typeof(queue.bind("ex", "rk", no_wait: true)).should eq(Amqp::Queue)
    typeof(queue.bind("ex", "rk", args: {ttl: 1_i32})).should eq(Amqp::Queue)
    typeof(queue.unbind("ex", "rk")).should eq(Amqp::Queue)
    typeof(queue.unbind("ex", "rk", args: {ttl: 1_i32})).should eq(Amqp::Queue)
    typeof(queue.publish("x")).should eq(UInt64)
    typeof(queue.publish(IO::Memory.new("x"), 1)).should eq(UInt64)
    typeof(queue.publish(IO::Memory.new("x"), 1) { |ok| ok.to_s; nil }).should eq(UInt64)
    typeof(queue.publish_confirm("x")).should eq(Bool)
    typeof(queue.publish_confirm(IO::Memory.new("x"), 1)).should eq(Bool)
    typeof(queue.get).should eq(Amqp::GetMessage?)
    typeof(queue.subscribe { |msg| msg.ack }).should eq(String)
    typeof(queue.subscribe(args: {mode: "fast"}) { |msg| msg.ack }).should eq(String)
    typeof(queue.unsubscribe("ctag")).should eq(Amqp::Queue)
    typeof(queue.purge).should eq(UInt32)
    typeof(queue.delete).should eq(UInt32)
    typeof(queue.message_count).should eq(UInt32)
    typeof(queue.consumer_count).should eq(UInt32)

    typeof(exchange.name).should eq(String)
    typeof(exchange.bind("src", "rk")).should eq(Amqp::Exchange)
    typeof(exchange.bind("src", "rk", no_wait: true)).should eq(Amqp::Exchange)
    typeof(exchange.bind("src", "rk", args: {ttl: 1_i32})).should eq(Amqp::Exchange)
    typeof(exchange.unbind("src", "rk")).should eq(Amqp::Exchange)
    typeof(exchange.unbind("src", "rk", no_wait: true)).should eq(Amqp::Exchange)
    typeof(exchange.unbind("src", "rk", args: {ttl: 1_i32})).should eq(Amqp::Exchange)
    typeof(exchange.publish("x", "rk")).should eq(UInt64)
    typeof(exchange.publish_confirm("x", "rk")).should eq(Bool)
    typeof(exchange.delete).should eq(Nil)
  end

  it "type-checks prepared publishers" do
    publisher = uninitialized Amqp::PreparedPublisher
    typeof(publisher.channel).should eq(Amqp::Channel)
    typeof(publisher.exchange).should eq(String)
    typeof(publisher.routing_key).should eq(String)
    typeof(publisher.properties).should eq(Amqp::Properties)
    typeof(publisher.mandatory).should eq(Bool)
    typeof(publisher.immediate).should eq(Bool)
    typeof(publisher.publish("x")).should eq(UInt64?)
    typeof(publisher.publish("x".to_slice)).should eq(UInt64?)
    typeof(publisher.publish_batch(["x".to_slice])).should eq(Array(UInt64?))
  end

  it "type-checks returned messages" do
    msg = uninitialized Amqp::ReturnedMessage
    typeof(msg.reply_code).should eq(UInt16)
    typeof(msg.reply_text).should eq(String)
    typeof(msg.exchange).should eq(String)
    typeof(msg.routing_key).should eq(String)
    typeof(msg.properties).should eq(Amqp::Properties)
    typeof(msg.body).should eq(Bytes)
    typeof(msg.reason).should eq(Amqp::ReturnReason)
  end

  it "type-checks amqp-client compatibility facade used by LavinMQ corridors" do
    client = AMQP::Client.new(URI.parse("amqp://guest:guest@127.0.0.1/"))
    typeof(client.connect).should eq(AMQP::Client::Connection)

    conn = uninitialized AMQP::Client::Connection
    typeof(conn.channel).should eq(AMQP::Client::Channel)
    typeof(conn.close(no_wait: false)).should eq(Nil)
    typeof(conn.closed?).should eq(Bool)
    raw_consume = AMQP::Client::Frame::Basic::Consume.new(
      1_u16, 0_u16, "q", "", false, true, false, true, AMQP::Client::Arguments.new)
    typeof(conn.write(raw_consume)).should eq(Nil)

    ch = uninitialized AMQP::Client::Channel
    typeof(ch.queue_declare("q")[:queue_name]).should eq(String)
    typeof(ch.queue_declare("q")[:message_count]).should eq(UInt32)
    typeof(ch.queue("q", durable: true, auto_delete: false, args: AMQP::Client::Arguments.new)).should eq(AMQP::Client::Queue)
    typeof(ch.queue("q", durable: true, auto_delete: false, args: {ttl: 1000_i32})).should eq(AMQP::Client::Queue)
    typeof(ch.queue_bind("q", "ex", "rk", AMQP::Client::Arguments.new)).should eq(Nil)
    typeof(ch.queue_bind("q", "ex", "rk", no_wait: true, args: {ttl: 1000_i32})).should eq(Nil)
    typeof(ch.prefetch(count: 1_u16)).should eq(Nil)
    typeof(ch.confirm_select).should eq(Nil)
    typeof(ch.wait_for_confirms).should eq(Bool)
    typeof(ch.basic_publish(IO::Memory.new("x"), "", "rk")).should eq(UInt64)
    typeof(ch.basic_publish(IO::Memory.new("x"), "", "rk") { nil }).should eq(UInt64)
    typeof(ch.basic_consume("q", no_ack: true, exclusive: false, block: true, args: AMQP::Client::Arguments.new, tag: "ctag") { |msg| msg.body_io; nil }).should eq(String)
    typeof(ch.basic_consume("q", no_ack: true, exclusive: false, block: true, args: {ttl: 1000_i32}, tag: "ctag") { |msg| msg.body_io; nil }).should eq(String)
    typeof(ch.basic_ack(1_u64, multiple: true)).should eq(Nil)

    q = uninitialized AMQP::Client::Queue
    typeof(q.bind("", "rk")).should eq(AMQP::Client::Queue)
    typeof(q.bind("", "rk", args: {ttl: 1000_i32})).should eq(AMQP::Client::Queue)
    typeof(q.subscribe(tag: "c", no_ack: true, block: true) { |msg| msg.body_io.to_slice; nil }).should eq(String)
    typeof(q.subscribe(tag: "c", no_ack: true, block: true, args: {ttl: 1000_i32}) { |msg| msg.body_io.to_slice; nil }).should eq(String)

    ex = uninitialized AMQP::Client::Exchange
    typeof(ex.unbind("src", "rk", AMQP::Client::Arguments.new)).should eq(AMQP::Client::Exchange)
    typeof(ex.unbind("src", "rk", no_wait: true, args: {ttl: 1000_i32})).should eq(AMQP::Client::Exchange)

    msg = uninitialized AMQP::Client::DeliverMessage
    typeof(msg.body_io).should eq(IO::Memory)
  end

  it "type-checks Subscription#receive as a select arm" do
    typeof(begin
      sub = uninitialized Amqp::Subscription
      select
      when msg = sub.receive
        msg
      when timeout(1.nanosecond)
        nil
      end
    end).should eq(Amqp::Delivery | Nil)
  end
end
