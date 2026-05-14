require "./spec_helper"

describe Amqp::Stats do
  it "starts at zero across every counter" do
    s = Amqp::Stats.new
    snap = s.snapshot
    snap.published.should eq(0_i64)
    snap.confirmed_ack.should eq(0_i64)
    snap.confirmed_nack.should eq(0_i64)
    snap.returned.should eq(0_i64)
    snap.consumed.should eq(0_i64)
    snap.recoveries_attempted.should eq(0_i64)
    snap.recoveries_succeeded.should eq(0_i64)
    snap.recoveries_failed.should eq(0_i64)
  end

  it "exposes a snapshot that captures all counters" do
    s = Amqp::Stats.new
    snap = s.snapshot
    snap.should be_a(Amqp::Stats::Snapshot)
  end
end

describe "Amqp::Connection#stats" do
  describe "(live broker)" do
    it "counts publishes, confirms, and consumed deliveries" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url) do |conn|
        ch = conn.open_channel
        ch.confirm_select
        info = ch.queue_declare(exclusive: true)
        sub = ch.consume(info.name, no_ack: true)
        3.times do |i|
          ch.publish("", info.name, "msg-#{i}".to_slice)
        end
        ch.wait_for_confirms(3.seconds).should be_true
        3.times { sub.receive }

        snap = conn.stats.snapshot
        snap.published.should eq(3_i64)
        snap.confirmed_ack.should eq(3_i64)
        snap.confirmed_nack.should eq(0_i64)
        snap.consumed.should eq(3_i64)
      end
    end

    it "counts recoveries_attempted and recoveries_succeeded" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 5) do |conn|
        ch = conn.open_channel
        ch.queue_declare("amqp-ng-stats-#{Random::Secure.hex(4)}",
          durable: false, auto_delete: true)

        conn.__force_disconnect_for_test
        deadline = Time.monotonic + 10.seconds
        loop do
          break if conn.recovered_to_open?
          fail "recovery did not complete" if Time.monotonic > deadline
          sleep 50.milliseconds
        end

        snap = conn.stats.snapshot
        snap.recoveries_attempted.should eq(1_i64)
        snap.recoveries_succeeded.should eq(1_i64)
        snap.recoveries_failed.should eq(0_i64)
      end
    end
  end
end
