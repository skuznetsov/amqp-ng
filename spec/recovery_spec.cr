require "./spec_helper"

# Wait until the connection re-enters the Open state (or until the
# deadline). Recovery happens on a separate fiber, so callers need to
# poll rather than block.
private def wait_for_recovery(conn : Amqp::Connection, deadline : Time::Span = 10.seconds) : Bool
  start = Time.monotonic
  loop do
    return true unless conn.closed? == false && conn.state_recovering?
    return false if Time.monotonic - start > deadline
    sleep 50.milliseconds
  end
end

describe "topology recovery" do
  describe "(live broker)" do
    it "recovers from forced socket close and resumes publish/consume" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true, heartbeat: 60.seconds,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 10) do |conn|
        ch = conn.open_channel
        q_name = "amqp-ng-recovery-test-#{Random::Secure.hex(4)}"
        info = ch.queue_declare(q_name, durable: false, exclusive: false, auto_delete: true)
        ch.qos(prefetch_count: 8_u16)
        sub = ch.consume(info.name, no_ack: true)

        # Yank the socket out from under the connection.
        conn.__force_disconnect_for_test

        # Wait for recovery to land us back in Open state.
        deadline = Time.monotonic + 10.seconds
        loop do
          break if conn.recovered_to_open?
          if Time.monotonic > deadline
            fail "connection did not recover within 10s (closed=#{conn.closed?})"
          end
          sleep 100.milliseconds
        end

        # Topology replay should have re-created the consumer with the
        # same tag, so a fresh publish flows end-to-end through `sub`.
        ch.publish("", info.name, "post-recovery".to_slice)
        d = sub.receive
        String.new(d.body).should eq("post-recovery")
        d.consumer_tag.should eq(sub.consumer_tag)
      end
    end

    it "replays exchange + binding + qos + confirms across recovery" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 10) do |conn|
        ch = conn.open_channel
        ex_name = "amqp-ng-recovery-test-#{Random::Secure.hex(4)}"
        ch.exchange_declare(ex_name, type: "direct", auto_delete: true)
        info = ch.queue_declare(exclusive: true)
        ch.queue_bind(info.name, ex_name, routing_key: "k")
        ch.qos(prefetch_count: 4_u16)
        ch.confirm_select
        sub = ch.consume(info.name, no_ack: true)

        conn.__force_disconnect_for_test

        deadline = Time.monotonic + 10.seconds
        loop do
          break if conn.recovered_to_open?
          fail "connection did not recover within 10s" if Time.monotonic > deadline
          sleep 100.milliseconds
        end

        ch.confirms?.should be_true
        seq = ch.publish(ex_name, "k", "routed".to_slice)
        seq.should eq(1_u64) # confirm-seq resets across recovery
        ch.wait_for_confirms(3.seconds).should be_true
        d = sub.receive
        String.new(d.body).should eq("routed")
      end
    end

    it "does NOT recover on authentication failure" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      expect_raises(Amqp::AuthenticationError) do
        Amqp.connect(SpecHelper.amqp_url, recovery: true, user: "no-such-user-#{Random::Secure.hex(4)}", password: "x") do |_|
          # unreachable
        end
      end
    end

    it "fails open_channel with RecoveryInProgress during recovery window" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      # Use a longer initial delay so we have time to observe the Recovering state.
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 2.seconds,
        recovery_max_attempts: 5) do |conn|
        ch = conn.open_channel
        ch.queue_declare(exclusive: true)

        conn.__force_disconnect_for_test

        # Poll until we observe the Recovering state.
        observed_recovering = false
        deadline = Time.monotonic + 3.seconds
        while Time.monotonic < deadline
          if conn.state_recovering?
            observed_recovering = true
            break
          end
          sleep 20.milliseconds
        end
        observed_recovering.should be_true

        expect_raises(Amqp::RecoveryInProgress) do
          conn.open_channel
        end

        # Eventually recovery completes.
        deadline = Time.monotonic + 10.seconds
        loop do
          break if conn.recovered_to_open?
          fail "connection did not recover" if Time.monotonic > deadline
          sleep 100.milliseconds
        end
      end
    end
  end
end
