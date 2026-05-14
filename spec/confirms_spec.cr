require "./spec_helper"

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
  end
end
