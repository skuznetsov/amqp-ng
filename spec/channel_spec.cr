require "./spec_helper"

describe Amqp::Channel do
  describe "(live broker)" do
    it "opens and closes a channel" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        ch.id.should be > 0_u16
        ch.closed?.should be_false
        ch.close
        ch.closed?.should be_true
      end
    end

    it "declares a server-named queue and reports counts" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        info.name.size.should be > 0
        info.message_count.should eq(0_u32)
        info.consumer_count.should eq(0_u32)
        ch.close
      end
    end

    it "publishes and consumes a small message" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.publish("", info.name, "hello".to_slice,
          Amqp::Properties.new(
            content_type: "text/plain",
            delivery_mode: Amqp::Properties::Persistence::Persistent,
          ))

        sub = ch.consume(info.name)
        delivery = sub.receive
        String.new(delivery.body).should eq("hello")
        delivery.exchange.should eq("")
        delivery.routing_key.should eq(info.name)
        delivery.properties.content_type.should eq("text/plain")
        delivery.properties.delivery_mode.should eq(Amqp::Properties::Persistence::Persistent)
        delivery.ack
        sub.close
        ch.close
      end
    end

    it "publishes and consumes a multi-frame body" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, frame_max: 4096_u32) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        body = String.build do |s|
          # 12 KB body → 3+ body frames at frame_max=4096
          12.times { s << "abcdefghij" * 100 }
        end.to_slice
        ch.publish("", info.name, body)
        sub = ch.consume(info.name, no_ack: true)
        delivery = sub.receive
        delivery.body.size.should eq(body.size)
        delivery.body.should eq(body)
        sub.close
        ch.close
      end
    end

    it "qos + prefetch_count limits concurrent deliveries" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.qos(prefetch_count: 1_u16)
        3.times { |i| ch.publish("", info.name, "msg-#{i}".to_slice) }

        sub = ch.consume(info.name)
        d1 = sub.receive
        String.new(d1.body).should eq("msg-0")
        d1.ack
        d2 = sub.receive
        String.new(d2.body).should eq("msg-1")
        d2.ack
        ch.close
      end
    end
  end
end
