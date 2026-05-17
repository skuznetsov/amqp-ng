require "./spec_helper"

class Amqp::Channel
  def __spec_register_pending_confirm(outcome : ::Channel(Amqp::ConfirmOutcome)? = nil,
                                      *,
                                      mandatory : Bool = false,
                                      message : Amqp::Message = Amqp::Message.new("spec"),
                                      exchange : String = "",
                                      routing_key : String = "spec") : UInt64
    @confirms_mutex.synchronize do
      register_pending_confirm_locked(message, exchange, routing_key, mandatory, outcome)
    end
  end

  def __spec_register_sync_pending_confirm(*,
                                           mandatory : Bool = false,
                                           message : Amqp::Message = Amqp::Message.new("spec"),
                                           exchange : String = "",
                                           routing_key : String = "spec") : UInt64
    @confirms_mutex.synchronize do
      register_sync_pending_confirm_locked(message, exchange, routing_key, mandatory)
    end
  end

  def __spec_settle_publish(tag : UInt64, *, multiple : Bool, nacked : Bool) : Nil
    __settle_publish_for_test(tag, multiple, nacked)
  end

  def __spec_await_sync_publish_confirm(tag : UInt64,
                                        timeout : Time::Span,
                                        exchange : String = "",
                                        routing_key : String = "spec") : Bool
    await_publish_confirm(tag, exchange, routing_key, timeout)
  end

  def __spec_await_publish_confirm(tag : UInt64,
                                   outcome : ::Channel(Amqp::ConfirmOutcome),
                                   timeout : Time::Span) : Bool
    __await_publish_confirm_for_test(tag, outcome, timeout)
  end

  def __spec_pending_confirm_count : Int32
    @confirms_mutex.synchronize { @pending_confirms.size }
  end

  def __spec_sync_confirm_waiter_count : Int32
    @confirms_mutex.synchronize { @pending_confirms.values.count(&.sync_waiter) }
  end

  def __spec_completed_sync_confirm_count : Int32
    @confirms_mutex.synchronize do
      @completed_sync_confirms.size + (@completed_sync_confirm_tag ? 1 : 0)
    end
  end

  def __spec_pending_replay_payload_sizes : Array(Int32?)
    @confirms_mutex.synchronize do
      @pending_confirms.values
        .sort_by(&.original_tag)
        .map { |pending| pending.replay_message.try &.body.size }
    end
  end

  def __spec_lowest_unconfirmed_tag : UInt64?
    @confirms_mutex.synchronize { @lowest_unconfirmed }
  end

  def __spec_abort_with(reason : Exception) : Nil
    abort_with(reason)
  end
end

describe "publisher confirms" do
  describe "(live broker)" do
    it "confirm_select flips channel into confirm mode" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        ch.confirms?.should be_false
        ch.confirm_select
        ch.confirms?.should be_true
        # idempotent
        ch.confirm_select
        ch.close
      end
    end

    it "publish returns seq numbers monotonically in confirm mode" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        seq1 = ch.publish("", info.name, "a".to_slice)
        seq2 = ch.publish("", info.name, "b".to_slice)
        seq3 = ch.publish("", info.name, "c".to_slice)
        seq1.should eq(1_u64)
        seq2.should eq(2_u64)
        seq3.should eq(3_u64)
        ch.wait_for_confirms.should be_true
        ch.close
      end
    end

    it "publish_batch returns seq numbers monotonically in confirm mode" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        seqs = ch.publish_batch([
          "batch-a".to_slice,
          "batch-b".to_slice,
          "batch-c".to_slice,
        ], "", info.name)
        seqs.should eq([1_u64, 2_u64, 3_u64])
        ch.wait_for_confirms(5.seconds).should be_true
        ch.close
      end
    end

    it "publish_confirm_batch confirms messages in bounded windows" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select

        messages = (0...5).map { |i| Amqp::Message.new("window-#{i}") }
        ch.publish_confirm_batch(messages, "", info.name, window_size: 2, timeout: 5.seconds).should be_true
        ch.__spec_pending_confirm_count.should eq(0)

        seen = [] of String
        5.times do
          msg = ch.get(info.name, auto_ack: true).not_nil!
          seen << String.new(msg.body)
        end
        seen.sort.should eq((0...5).map { |i| "window-#{i}" })
        ch.close
      end
    end

    it "publish_confirm_batch rejects non-positive window sizes" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select

        expect_raises(ArgumentError, "window_size must be positive") do
          ch.publish_confirm_batch([Amqp::Message.new("bad-window")], "", info.name, window_size: 0)
        end
        ch.__spec_pending_confirm_count.should eq(0)
        ch.close
      end
    end

    it "does not retain replay payloads when recovery is disabled" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: false) do |conn|
        ch = conn.open_channel
        ch.confirm_select
        body = Bytes.new(4096, 1_u8)

        ch.__spec_register_pending_confirm(message: Amqp::Message.new(body))

        ch.__spec_pending_replay_payload_sizes.should eq([nil])
        ch.close
      end
    end

    it "maintains a low-watermark for wait_for_confirms progress checks" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        ch.confirm_select
        tag1 = ch.__spec_register_pending_confirm
        tag2 = ch.__spec_register_pending_confirm
        tag3 = ch.__spec_register_pending_confirm

        ch.__spec_lowest_unconfirmed_tag.should eq(tag1)
        ch.__spec_settle_publish(tag1, multiple: false, nacked: false)
        ch.__spec_lowest_unconfirmed_tag.should eq(tag2)
        ch.__spec_settle_publish(tag3, multiple: true, nacked: false)
        ch.__spec_lowest_unconfirmed_tag.should be_nil
        ch.wait_for_confirms.should be_true
        ch.close
      end
    end

    it "publish without confirm mode returns nil" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.publish("", info.name, "no-confirm".to_slice).should be_nil
        ch.close
      end
    end

    it "wait_for_confirms with no outstanding publishes returns true" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        ch.confirm_select
        ch.wait_for_confirms.should be_true
        ch.close
      end
    end

    it "wait_for_confirms raises if used outside confirm mode" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        expect_raises(Amqp::ConcurrencyError, /confirm mode/) do
          ch.wait_for_confirms
        end
        ch.close
      end
    end

    it "publish_confirm requires confirm mode before writing frames" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        expect_raises(Amqp::ConfigurationError) do
          ch.publish_confirm(Amqp::Message.new("must-not-publish"), "", info.name)
        end
        ch.get(info.name).should be_nil
        ch.close
      end
    end

    it "publish_async requires confirm mode before writing frames" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        expect_raises(Amqp::ConfigurationError) do
          ch.publish_async(Amqp::Message.new("must-not-publish"), "", info.name)
        end
        ch.get(info.name).should be_nil
        ch.close
      end
    end

    it "wait_for_confirms acks a large batch (multiple=true compaction)" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        50.times { |i| ch.publish("", info.name, "msg-#{i}".to_slice) }
        ch.wait_for_confirms(5.seconds).should be_true
        ch.close
      end
    end

    it "publish_confirm raises PublishReturnedError for mandatory unroutable publishes" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        ex = "amqp-ng-return-ex-#{Random::Secure.hex(4)}"
        ch.exchange_declare(ex, "direct", auto_delete: true)
        err = expect_raises(Amqp::PublishReturnedError) do
          ch.publish_confirm(Amqp::Message.new("unroutable"), ex, "missing-rk", mandatory: true, timeout: 5.seconds)
        end
        err.reason.reply_code.should eq(312_u16)
        err.reason.exchange.should eq(ex)
        ch.exchange_delete(ex)
        ch.close
      end
    end

    it "publish_confirm returns true for acknowledged routable publishes" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        ch.publish_confirm(Amqp::Message.new("confirmed"), "", info.name, mandatory: true, timeout: 5.seconds).should be_true
        got = ch.get(info.name)
        got.should_not be_nil
        String.new(got.not_nil!.body).should eq("confirmed")
        ch.close
      end
    end

    it "publish_async resolves returned mandatory publishes as Returned outcome" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        ex = "amqp-ng-return-async-ex-#{Random::Secure.hex(4)}"
        ch.exchange_declare(ex, "direct", auto_delete: true)
        tag, outcome_ch = ch.publish_async(Amqp::Message.new("unroutable"), ex, "missing-rk", mandatory: true)
        outcome = nil
        select
        when outcome = outcome_ch.receive?
        when timeout(5.seconds)
          fail "timed out waiting for async return outcome"
        end
        outcome.should_not be_nil
        outcome.not_nil!.delivery_tag.should eq(tag)
        outcome.not_nil!.kind.returned?.should be_true
        outcome.not_nil!.return_reason.not_nil!.exchange.should eq(ex)
        ch.exchange_delete(ex)
        ch.close
      end
    end

    it "publish_async resolves acknowledged publishes exactly once" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        tag, outcome_ch = ch.publish_async(Amqp::Message.new("async-confirmed"), "", info.name, mandatory: true)
        outcome = nil
        select
        when outcome = outcome_ch.receive?
        when timeout(5.seconds)
          fail "timed out waiting for async ack outcome"
        end
        outcome.should_not be_nil
        outcome.not_nil!.delivery_tag.should eq(tag)
        outcome.not_nil!.kind.ack?.should be_true
        outcome_ch.receive?.should be_nil
        got = ch.get(info.name)
        got.should_not be_nil
        String.new(got.not_nil!.body).should eq("async-confirmed")
        ch.close
      end
    end

    it "keeps mandatory return ordering separate from following acked publishes" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        ex = "amqp-ng-return-order-ex-#{Random::Secure.hex(4)}"
        info = ch.queue_declare(exclusive: true)
        ch.exchange_declare(ex, "direct", auto_delete: true)
        ch.queue_bind(info.name, ex, "ok")

        returned_tag, returned_ch = ch.publish_async(
          Amqp::Message.new("returned"), ex, "missing", mandatory: true)
        acked_tag, acked_ch = ch.publish_async(
          Amqp::Message.new("acked"), ex, "ok", mandatory: true)

        returned = returned_ch.receive?
        acked = acked_ch.receive?

        returned.should_not be_nil
        returned.not_nil!.delivery_tag.should eq(returned_tag)
        returned.not_nil!.kind.returned?.should be_true
        returned.not_nil!.return_reason.not_nil!.routing_key.should eq("missing")

        acked.should_not be_nil
        acked.not_nil!.delivery_tag.should eq(acked_tag)
        acked.not_nil!.kind.ack?.should be_true

        got = ch.get(info.name)
        got.should_not be_nil
        String.new(got.not_nil!.body).should eq("acked")

        ch.exchange_delete(ex)
        ch.close
      end
    end

    it "publish_confirm with zero timeout raises before writing frames" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        err = expect_raises(Amqp::PublishTimeoutError) do
          ch.publish_confirm(Amqp::Message.new("must-not-publish"), "", info.name, timeout: 0.seconds)
        end
        err.delivery_tag.should eq(0_u64)
        ch.get(info.name).should be_nil
        ch.close
      end
    end

    it "raises PublishOutOfOrderError for broker ack of an unknown publish tag" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        expect_raises(Amqp::PublishOutOfOrderError) do
          ch.__spec_settle_publish(99_u64, multiple: false, nacked: false)
        end
        expect_raises(Amqp::PublishOutOfOrderError) do
          ch.__spec_settle_publish(99_u64, multiple: true, nacked: false)
        end
        ch.close
      end
    end

    it "settles broker nacks as Nack outcomes and failed wait_for_confirms" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        outcome_ch = ::Channel(Amqp::ConfirmOutcome).new(1)
        tag = ch.__spec_register_pending_confirm(outcome_ch)

        ch.__spec_settle_publish(tag, multiple: false, nacked: true)

        outcome = outcome_ch.receive?
        outcome.should_not be_nil
        outcome.not_nil!.delivery_tag.should eq(tag)
        outcome.not_nil!.kind.nack?.should be_true
        outcome_ch.receive?.should be_nil
        ch.wait_for_confirms.should be_false
        ch.close
      end
    end

    it "settles multiple=true broker nacks for every pending tag in range" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        outcomes = Array(::Channel(Amqp::ConfirmOutcome)).new
        tags = 3.times.map do
          outcome_ch = ::Channel(Amqp::ConfirmOutcome).new(1)
          outcomes << outcome_ch
          ch.__spec_register_pending_confirm(outcome_ch)
        end.to_a

        ch.__spec_settle_publish(tags.last, multiple: true, nacked: true)

        outcomes.zip(tags).each do |outcome_ch, tag|
          outcome = outcome_ch.receive?
          outcome.should_not be_nil
          outcome.not_nil!.delivery_tag.should eq(tag)
          outcome.not_nil!.kind.nack?.should be_true
          outcome_ch.receive?.should be_nil
        end
        ch.wait_for_confirms.should be_false
        ch.close
      end
    end

    it "does not block the confirm tracker when an async outcome is never received" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        unread_outcome = ::Channel(Amqp::ConfirmOutcome).new(1)
        tag = ch.__spec_register_pending_confirm(unread_outcome)
        settled = ::Channel(Nil).new(1)

        spawn do
          ch.__spec_settle_publish(tag, multiple: false, nacked: false)
          settled.send(nil)
        end

        select
        when settled.receive
        when timeout(1.second)
          fail "confirm tracker blocked on an unread async outcome channel"
        end

        ch.__spec_pending_confirm_count.should eq(0)
        ch.close
      end
    end

    it "closes async outcome channels without a value when the channel aborts" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        outcome_ch = ::Channel(Amqp::ConfirmOutcome).new(1)
        ch.__spec_register_pending_confirm(outcome_ch)

        ch.__spec_abort_with(Amqp::ChannelClosedByCaller.new("spec abort"))

        outcome_ch.receive?.should be_nil
        ch.__spec_pending_confirm_count.should eq(0)
      end
    end

    it "keeps timed-out publish entries until a late ack cleans them up" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        outcome_ch = ::Channel(Amqp::ConfirmOutcome).new(1)
        tag = ch.__spec_register_pending_confirm(outcome_ch)

        err = expect_raises(Amqp::PublishTimeoutError) do
          ch.__spec_await_publish_confirm(tag, outcome_ch, 1.nanosecond)
        end
        err.delivery_tag.should eq(tag)
        ch.__spec_pending_confirm_count.should eq(1)

        ch.__spec_settle_publish(tag, multiple: false, nacked: false)

        outcome = outcome_ch.receive?
        outcome.should_not be_nil
        outcome.not_nil!.kind.ack?.should be_true
        ch.__spec_pending_confirm_count.should eq(0)
        ch.close
      end
    end

    it "settles synchronous publish_confirm waiters without per-publish outcome channels" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        tag = ch.__spec_register_sync_pending_confirm

        ch.__spec_settle_publish(tag, multiple: false, nacked: false)

        ch.__spec_await_sync_publish_confirm(tag, 5.seconds).should be_true
        ch.__spec_sync_confirm_waiter_count.should eq(0)
        ch.__spec_completed_sync_confirm_count.should eq(0)
        ch.__spec_pending_confirm_count.should eq(0)
        ch.close
      end
    end

    it "allows concurrent publish_confirm calls on one channel without tearing frames" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        info = ch.queue_declare(exclusive: true)
        ch.confirm_select
        done = ::Channel(Exception?).new(8)

        8.times do |i|
          spawn do
            begin
              ch.publish_confirm("confirm-#{i}".to_slice, "", info.name, timeout: 5.seconds).should be_true
              done.send(nil)
            rescue ex
              done.send(ex)
            end
          end
        end

        8.times do
          if ex = done.receive
            raise ex
          end
        end
        ch.__spec_pending_confirm_count.should eq(0)
        ch.close
      end
    end

    it "does not retain completed sync confirm results after a timeout abandons the waiter" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        tag = ch.__spec_register_sync_pending_confirm

        expect_raises(Amqp::PublishTimeoutError) do
          ch.__spec_await_sync_publish_confirm(tag, 1.nanosecond)
        end
        ch.__spec_sync_confirm_waiter_count.should eq(0)
        ch.__spec_pending_confirm_count.should eq(1)

        ch.__spec_settle_publish(tag, multiple: false, nacked: false)

        ch.__spec_pending_confirm_count.should eq(0)
        ch.__spec_completed_sync_confirm_count.should eq(0)
        ch.close
      end
    end

    it "wakes a publish_confirm waiter when the channel closes mid-wait" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.channel
        ch.confirm_select
        outcome_ch = ::Channel(Amqp::ConfirmOutcome).new(1)
        tag = ch.__spec_register_pending_confirm(outcome_ch)
        done = ::Channel(Exception?).new(1)

        spawn do
          begin
            ch.__spec_await_publish_confirm(tag, outcome_ch, 5.seconds)
            done.send(nil)
          rescue ex
            done.send(ex)
          end
        end
        Fiber.yield

        ch.__spec_abort_with(Amqp::ChannelClosedByCaller.new("spec close"))

        ex = done.receive
        ex.should be_a(Amqp::ChannelClosedByCaller)
        ch.__spec_pending_confirm_count.should eq(0)
      end
    end
  end
end
