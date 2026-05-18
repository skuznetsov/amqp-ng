require "./spec_helper"

class ::Channel(T)
  def __spec_buffer_depth : Int32
    queue = @queue
    queue ? queue.size : 0
  end
end

class Amqp::Subscription
  def self.__spec_new(capacity : Int32 = 16) : Amqp::Subscription
    new(Amqp::Channel.allocate, "spec-consumer", "spec-queue", capacity)
  end

  def __spec_deliver(body : String, delivery_tag : UInt64 = 1_u64) : Nil
    deliver(Amqp::Delivery.new(
      @consumer_tag,
      delivery_tag,
      false,
      "",
      @queue,
      Amqp::Properties.new,
      body.to_slice.dup,
    ))
  end

  def __spec_mark_closed : Nil
    mark_closed
  end

  def __spec_buffer_depth : Int32
    @mailbox.__spec_buffer_depth
  end
end

describe Amqp::Subscription do
  it "receives buffered deliveries through a select arm" do
    sub = Amqp::Subscription.__spec_new
    spawn { sub.__spec_deliver("selected") }

    received = nil
    select
    when msg = sub.receive
      received = String.new(msg.body)
    when timeout(1.second)
      fail "timed out waiting for subscription delivery"
    end

    received.should eq("selected")
  end

  it "drains buffered deliveries after close before raising" do
    sub = Amqp::Subscription.__spec_new
    sub.__spec_deliver("a", 1_u64)
    sub.__spec_deliver("b", 2_u64)

    sub.__spec_mark_closed

    sub.closed?.should be_true
    String.new(sub.receive.body).should eq("a")
    String.new(sub.receive.body).should eq("b")
    sub.receive?.should be_nil
    expect_raises(Amqp::SubscriptionClosed) { sub.receive }
  end

  it "receive? returns nil after close with no buffered deliveries" do
    sub = Amqp::Subscription.__spec_new

    sub.__spec_mark_closed

    sub.receive?.should be_nil
    expect_raises(Amqp::SubscriptionClosed) { sub.receive }
  end

  describe "(live broker)" do
    it "caller close cancels the consumer and closes the subscription" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        sub = ch.subscribe(info.name, auto_ack: true)

        sub.close

        sub.closed?.should be_true
        sub.receive?.should be_nil
        ch.close
      end
    end

    it "broker-side queue deletion cancels the subscription" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        consumer = conn.channel
        admin = conn.channel
        info = consumer.queue_declare(exclusive: false, auto_delete: false)
        sub = consumer.subscribe(info.name, auto_ack: true)

        admin.queue_delete(info.name)

        deadline = Time.instant + 2.seconds
        until sub.closed?
          fail "subscription was not cancelled by broker" if Time.instant > deadline
          sleep 10.milliseconds
        end
        sub.receive?.should be_nil
        admin.close
        consumer.close
      end
    end

    it "routes deliveries to multiple consumers on one channel" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        left = ch.queue_declare(exclusive: true)
        right = ch.queue_declare(exclusive: true)
        left_sub = ch.subscribe(left.name, consumer_tag: "left-#{Random::Secure.hex(4)}", auto_ack: true)
        right_sub = ch.subscribe(right.name, consumer_tag: "right-#{Random::Secure.hex(4)}", auto_ack: true)

        3.times do |i|
          ch.publish("", left.name, "left-#{i}".to_slice)
          ch.publish("", right.name, "right-#{i}".to_slice)
        end

        3.times do |i|
          String.new(left_sub.receive.body).should eq("left-#{i}")
          String.new(right_sub.receive.body).should eq("right-#{i}")
        end
        ch.close
      end
    end

    it "a full subscription mailbox does not block unrelated channels on the connection" do
      pending! "set AMQP_BACKPRESSURE_LIVE=1 to run this live broker measurement" unless ENV["AMQP_BACKPRESSURE_LIVE"]?
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      conn = Amqp.connect(SpecHelper.amqp_url)
      publisher_conn = Amqp.connect(SpecHelper.amqp_url)
      begin
        slow = conn.channel
        probe = conn.channel
        publisher = publisher_conn.channel
        queue = "amqp-ng-bp-#{Random::Secure.hex(8)}"
        info = slow.queue_declare(queue, auto_delete: true)
        sub = slow.subscribe(info.name, auto_ack: true, buffer: 1)
        # This intentionally fills the subscription mailbox without
        # pushing further deliveries that would block the channel handler
        # and make cleanup destructive.
        published = 1

        published.times { |i| publisher.publish("", info.name, "bp-#{i}".to_slice) }

        fill_deadline = Time.instant + 2.seconds
        until sub.__spec_buffer_depth >= 1
          fail "subscription mailbox did not fill" if Time.instant > fill_deadline
          sleep 10.milliseconds
        end
        sleep 200.milliseconds

        probe_done = ::Channel(String).new
        spawn do
          probe.queue_declare(exclusive: true)
          probe_done.send("ok")
        rescue ex
          probe_done.send("#{ex.class}: #{ex.message}")
        end

        select
        when result = probe_done.receive
          result.should eq("ok")
        when timeout(1.second)
          fail "probe RPC on another channel was blocked by slow subscription"
        end
      ensure
        conn.__force_disconnect_for_test
        publisher_conn.__force_disconnect_for_test
      end
    end
  end
end
