require "./spec_helper"

class Amqp::Channel
  def __spec_set_flow_active(active : Bool) : Nil
    set_flow_active(active)
  end

  def __spec_flow_active? : Bool
    @flow_mutex.synchronize { @flow_active }
  end
end

describe Amqp::Channel do
  describe "(live broker)" do
    it "opens and closes a channel" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        ch.id.should be > 0_u16
        ch.closed?.should be_false
        ch.flow(true)
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

    it "publishes from an IO body with an explicit byte size" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        queue = ch.queue
        io = IO::Memory.new("io-body")

        queue.publish(io, 7)

        msg = queue.get.not_nil!
        String.new(msg.body).should eq("io-body")
        queue.delete
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

    it "does not ack again from block consume when auto_ack is broker no-ack" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.publish("", info.name, "no-ack-block".to_slice)

        delivered = ::Channel(Nil).new(1)
        failed = ::Channel(Exception?).new(1)
        closed = ::Channel(Tuple(UInt16, String)).new(1)
        ch.on_close do |code, text|
          closed.send({code, text})
        end

        spawn do
          begin
            ch.consume(info.name, auto_ack: true) do |delivery|
              String.new(delivery.body).should eq("no-ack-block")
              delivered.send(nil)
            end
            failed.send(nil)
          rescue ex
            failed.send(ex)
          end
        end

        select
        when delivered.receive
        when timeout(2.seconds)
          fail "timed out waiting for block consumer delivery"
        end

        select
        when event = closed.receive
          fail "channel closed after no-ack block delivery: #{event[0]} #{event[1]}"
        when timeout(200.milliseconds)
        end

        ch.close
        if ex = failed.receive
          raise ex
        end
      end
    end

    it "publishes a batch under one channel operation" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        seqs = ch.publish_batch([
          "batch-0".to_slice,
          "batch-1".to_slice,
          "batch-2".to_slice,
        ], "", info.name)
        seqs.should eq([nil, nil, nil])

        3.times do |i|
          msg = ch.get(info.name, auto_ack: true)
          msg.should_not be_nil
          String.new(msg.not_nil!.body).should eq("batch-#{i}")
        end
        ch.get(info.name).should be_nil
        ch.close
      end
    end

    it "publishes through a prepared fixed-route publisher" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        publisher = ch.prepared_publisher("", info.name,
          properties: Amqp::Properties.new(content_type: "text/plain"))

        publisher.publish("prepared-0")
        publisher.publish_batch([
          "prepared-1".to_slice,
          "prepared-2".to_slice,
        ]).should eq([nil, nil])

        3.times do |i|
          msg = ch.get(info.name, auto_ack: true)
          msg.should_not be_nil
          msg = msg.not_nil!
          String.new(msg.body).should eq("prepared-#{i}")
          msg.properties.content_type.should eq("text/plain")
        end
        ch.get(info.name).should be_nil
        ch.close
      end
    end

    it "serializes concurrent fire-and-forget publishes on one channel" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        total = 80
        done = ::Channel(Exception?).new(total)

        total.times do |i|
          spawn do
            begin
              ch.publish("", info.name, "concurrent-#{i}".to_slice)
              done.send(nil)
            rescue ex
              done.send(ex)
            end
          end
        end

        total.times do
          if ex = done.receive
            raise ex
          end
        end

        seen = Hash(String, Int32).new(0)
        total.times do
          msg = ch.get(info.name, auto_ack: true)
          msg.should_not be_nil
          seen[String.new(msg.not_nil!.body)] += 1
        end

        total.times do |i|
          seen["concurrent-#{i}"].should eq(1)
        end
        ch.get(info.name).should be_nil
        ch.close
      end
    end

    it "supports amqp-client.cr basic_* aliases for publish/get/ack" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)

        ch.basic_publish("compat", "", info.name).should eq(0_u64)
        msg = ch.basic_get(info.name, false)
        msg.should_not be_nil
        String.new(msg.not_nil!.body).should eq("compat")
        ch.basic_ack(msg.not_nil!.delivery_tag)
        ch.basic_get(info.name).should be_nil
        ch.close
      end
    end

    it "supports amqp-client.cr basic publish confirm aliases" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)

        ch.basic_publish_confirm("compat-confirm", "", info.name, timeout: 5.seconds).should be_true
        String.new(ch.basic_get(info.name).not_nil!.body).should eq("compat-confirm")

        callback = ::Channel(Bool).new(1)
        ch.basic_publish("compat-callback", "", info.name) do |ok|
          callback.send(ok)
        end.should be > 0_u64
        callback.receive.should be_true
        String.new(ch.basic_get(info.name).not_nil!.body).should eq("compat-callback")
        ch.close
      end
    end

    it "blocks amqp-client.cr callback publishes while channel.flow is inactive" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        done = ::Channel(Exception?).new(1)
        callback = ::Channel(Bool).new(1)

        ch.__spec_set_flow_active(false)
        spawn do
          begin
            ch.basic_publish("flow-callback", "", info.name) do |ok|
              callback.send(ok)
            end
            done.send(nil)
          rescue ex
            done.send(ex)
          end
        end

        select
        when result = done.receive
          raise result if result
          fail "callback publish completed while channel.flow was inactive"
        when timeout(50.milliseconds)
        end

        ch.__spec_set_flow_active(true)
        if ex = done.receive
          raise ex
        end
        callback.receive.should be_true
        String.new(ch.basic_get(info.name).not_nil!.body).should eq("flow-callback")
        ch.close
      end
    end

    it "rejects amqp-client.cr callback publishes after channel close" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        ch.close

        expect_raises(Amqp::ChannelClosedByCaller) do
          ch.basic_publish("closed-callback", "", info.name) { |_| nil }
        end
      end
    end

    it "supports amqp-client.cr queue and exchange wrappers" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        q = ch.queue
        ex = ch.exchange("amqp-ng-wrapper-ex-#{Random::Secure.hex(4)}",
          "direct", durable: false, auto_delete: true)

        q.bind(ex.name, "rk")
        ex.publish("wrapped", "rk").should eq(0_u64)
        got = nil
        deadline = Time.instant + 5.seconds
        until got || Time.instant >= deadline
          got = q.get
          sleep 10.milliseconds unless got
        end
        got.should_not be_nil
        String.new(got.not_nil!.body).should eq("wrapped")
        q.unbind(ex.name, "rk")

        q.publish_confirm("direct-wrapped", timeout: 5.seconds).should be_true
        String.new(q.get.not_nil!.body).should eq("direct-wrapped")
        q.delete
        ex.delete
        ch.close
      end
    end

    it "supports on_return for mandatory fire-and-forget publishes" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        ex = "amqp-ng-return-ex-#{Random::Secure.hex(4)}"
        ch.exchange_declare(ex, "direct", auto_delete: true)
        returned = ::Channel(Amqp::ReturnedMessage).new(1)
        ch.on_return { |msg| returned.send(msg) }

        ch.publish(ex, "missing-rk", "unroutable".to_slice,
          Amqp::Properties.new(message_id: "return-smoke"), mandatory: true)

        msg = nil
        select
        when msg = returned.receive
        when timeout(5.seconds)
          fail "timed out waiting for basic.return callback"
        end

        msg.should_not be_nil
        msg = msg.not_nil!
        msg.reply_code.should eq(312_u16)
        msg.exchange.should eq(ex)
        msg.routing_key.should eq("missing-rk")
        msg.properties.message_id.should eq("return-smoke")
        String.new(msg.body).should eq("unroutable")
        ch.exchange_delete(ex)
        ch.close
      end
    end

    it "supports basic.recover for unacked deliveries" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        q = ch.queue
        q.publish_confirm("recover-me", timeout: 5.seconds)

        first = q.get(no_ack: false)
        first.should_not be_nil
        String.new(first.not_nil!.body).should eq("recover-me")

        ch.basic_recover(requeue: true)
        second = q.get(no_ack: true)
        second.should_not be_nil
        String.new(second.not_nil!.body).should eq("recover-me")
        second.not_nil!.redelivered.should be_true
        q.delete
        ch.close
      end
    end

    it "blocks publishes while channel.flow is inactive and resumes when active" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        q = ch.queue
        done = ::Channel(Exception?).new(1)

        ch.__spec_set_flow_active(false)
        ch.__spec_flow_active?.should be_false
        spawn do
          begin
            ch.publish("", q.name, "flow-controlled".to_slice)
            done.send(nil)
          rescue ex
            done.send(ex)
          end
        end

        select
        when result = done.receive
          raise result if result
          fail "publish completed while channel.flow was inactive"
        when timeout(50.milliseconds)
        end

        ch.__spec_set_flow_active(true)
        ch.__spec_flow_active?.should be_true
        if ex = done.receive
          raise ex
        end
        String.new(q.get.not_nil!.body).should eq("flow-controlled")
        q.delete
        ch.close
      end
    end

    it "blocks raw-byte batch publishes while channel.flow is inactive" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        q = ch.queue
        done = ::Channel(Exception?).new(1)

        ch.__spec_set_flow_active(false)
        spawn do
          begin
            ch.publish_batch(["flow-batch".to_slice], "", q.name)
            done.send(nil)
          rescue ex
            done.send(ex)
          end
        end

        select
        when result = done.receive
          raise result if result
          fail "raw-byte batch publish completed while channel.flow was inactive"
        when timeout(50.milliseconds)
        end

        ch.__spec_set_flow_active(true)
        if ex = done.receive
          raise ex
        end
        String.new(q.get.not_nil!.body).should eq("flow-batch")
        q.delete
        ch.close
      end
    end

    it "rejects raw-byte batch publishes after channel close" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        q = ch.queue
        ch.close

        expect_raises(Amqp::ChannelClosedByCaller) do
          ch.publish_batch(["closed-batch".to_slice], "", q.name)
        end
      end
    end

    it "supports tx.commit and tx.rollback" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        q = ch.queue

        ch.tx_select
        q.publish("rolled-back")
        ch.tx_rollback
        q.get.should be_nil

        ch.transaction do
          q.publish("committed")
          "transaction-value"
        end.should eq("transaction-value")

        String.new(q.get.not_nil!.body).should eq("committed")
        q.delete
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

    it "gets a message synchronously and purges queues" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        ch.publish("", info.name, "one".to_slice)
        got = ch.get(info.name)
        got.should_not be_nil
        String.new(got.not_nil!.body).should eq("one")
        ch.publish("", info.name, "two".to_slice)
        ch.queue_purge(info.name).should eq(1_u32)
        ch.get(info.name).should be_nil
        ch.close
      end
    end

    it "surfaces broker channel.close during a passive declare RPC" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        missing = "amqp-ng-missing-#{Random::Secure.hex(8)}"
        closed = ::Channel(Tuple(UInt16, String)).new(1)
        ch.on_close do |code, text|
          closed.send({code, text})
        end

        err = expect_raises(Amqp::ChannelClosedByBroker) do
          ch.queue_declare(missing, passive: true)
        end

        err.reply_code.should eq(404_u16)
        ch.closed?.should be_true
        conn.closed?.should be_false
        select
        when event = closed.receive
          event[0].should eq(404_u16)
          event[1].should contain("NOT_FOUND")
        when timeout(2.seconds)
          fail "timed out waiting for on_close callback"
        end
      end
    end

    it "emits on_cancel when the broker cancels a consumer" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        consumer = conn.channel
        admin = conn.channel
        queue = "amqp-ng-cancel-#{Random::Secure.hex(8)}"
        cancelled = ::Channel(String).new(1)

        admin.queue_declare(queue, durable: false, exclusive: false, auto_delete: false)
        consumer.on_cancel do |tag|
          cancelled.send(tag)
        end
        sub = consumer.consume(queue, no_ack: true)
        admin.queue_delete(queue)

        select
        when tag = cancelled.receive
          tag.should eq(sub.consumer_tag)
        when timeout(2.seconds)
          fail "timed out waiting for on_cancel callback"
        end
        sub.closed?.should be_true
        consumer.close rescue nil
        admin.close rescue nil
      end
    end

    it "binds and unbinds queue and exchange topology" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        conn.with_channel do |ch|
          ex = "amqp-ng-api-ex-#{Random::Secure.hex(4)}"
          q = ch.queue_declare(exclusive: true)
          ch.exchange_declare(ex, "direct", auto_delete: true)
          ch.queue_bind(q.name, ex, "rk")
          ch.queue_unbind(q.name, ex, "rk")
          ch.exchange_delete(ex)
        end
      end
    end

    it "accepts NamedTuple arguments for topology helpers" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        conn.with_channel do |ch|
          ex = "amqp-ng-nt-ex-#{Random::Secure.hex(4)}"
          q = ch.queue_declare(exclusive: true, arguments: {marker: "namedtuple"})
          ch.exchange_declare(ex, "direct", auto_delete: true, arguments: {marker: "namedtuple"})
          ch.queue_bind(q.name, ex, "rk", {marker: "namedtuple"})
          ch.queue_unbind(q.name, ex, "rk", {marker: "namedtuple"})
          ch.exchange_delete(ex)
        end
      end
    end
  end
end
