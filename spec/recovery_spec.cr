require "./spec_helper"

class Amqp::Channel
  def __spec_topology_counts
    @topology_mutex.synchronize do
      {
        exchanges: @topology_exchanges.size,
        queues:    @topology_queues.size,
        bindings:  @topology_bindings.size,
        qos:       @topology_qos.nil? ? 0 : 1,
        consumers: @topology_consumers.size,
      }
    end
  end

  def __spec_register_pending_replay_confirm(outcome : ::Channel(Amqp::ConfirmOutcome),
                                             message : Amqp::Message,
                                             exchange : String,
                                             routing_key : String) : UInt64
    @confirms_mutex.synchronize do
      register_pending_confirm_locked(message, exchange, routing_key, false, outcome)
    end
  end

  def __spec_pending_recovery_replay_payload_sizes : Array(Int32?)
    @confirms_mutex.synchronize do
      @pending_confirms.values
        .sort_by(&.original_tag)
        .map { |pending| pending.replay_message.try &.body.size }
    end
  end

  def __spec_record_unreplayable_queue(name : String) : Nil
    @topology_mutex.synchronize do
      @topology_queues[name] = QueueOp.new(
        name, name, false, false, false, false, Amqp::Arguments.new,
      )
    end
  end

  def __spec_record_unreplayable_consumer(queue : String, consumer_tag : String) : Nil
    @topology_mutex.synchronize do
      @topology_consumers[consumer_tag] = ConsumeOp.new(
        queue, consumer_tag, false, true, false, Amqp::Arguments.new,
      )
    end
  end
end

# Wait until the connection re-enters the Open state (or until the
# deadline). Recovery happens on a separate fiber, so callers need to
# poll rather than block.
private def wait_for_recovery(conn : Amqp::Connection, deadline : Time::Span = 10.seconds) : Bool
  start = Time.instant
  loop do
    return true unless conn.closed? == false && conn.state_recovering?
    return false if Time.instant - start > deadline
    sleep 50.milliseconds
  end
end

private def receive_recovery_delivery(sub : Amqp::Subscription,
                                      timeout_span : Time::Span = 3.seconds) : Amqp::Delivery
  select
  when delivery = sub.receive
    delivery
  when timeout(timeout_span)
    fail "timed out waiting for recovered delivery"
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
        deadline = Time.instant + 10.seconds
        loop do
          break if conn.recovered_to_open?
          if Time.instant > deadline
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

    it "recovers from broker-forced connection close and resumes publish/consume" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true, heartbeat: 60.seconds,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 10) do |conn|
        ch = conn.open_channel
        q_name = "amqp-ng-recovery-forced-close-#{Random::Secure.hex(4)}"
        info = ch.queue_declare(q_name, durable: false, exclusive: false, auto_delete: true)
        sub = ch.consume(info.name, no_ack: true)

        conn.__force_broker_close_for_test

        deadline = Time.instant + 10.seconds
        loop do
          break if conn.recovered_to_open?
          if Time.instant > deadline
            fail "connection did not recover from broker-forced close within 10s (closed=#{conn.closed?}, reason=#{conn.close_reason.try(&.class)})"
          end
          sleep 100.milliseconds
        end

        conn.stats.snapshot.recoveries_succeeded.should eq(1)
        ch.publish("", info.name, "post-forced-close".to_slice)
        d = sub.receive
        String.new(d.body).should eq("post-forced-close")
      end
    end

    it "does not recover from non-forced broker connection close" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 3) do |conn|
        conn.__force_broker_close_for_test(404_u16, "NOT_FOUND - synthetic non-recoverable close")

        deadline = Time.instant + 2.seconds
        loop do
          break if conn.closed?
          fail "connection did not close after non-recoverable broker close" if Time.instant > deadline
          sleep 50.milliseconds
        end

        reason = conn.close_reason
        reason.should be_a(Amqp::ConnectionClosedByBroker)
        reason.as(Amqp::ConnectionClosedByBroker).reply_code.should eq(404_u16)
        conn.stats.snapshot.recoveries_attempted.should eq(0)
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

        deadline = Time.instant + 10.seconds
        loop do
          break if conn.recovered_to_open?
          fail "connection did not recover within 10s" if Time.instant > deadline
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

    it "re-publishes pending confirm messages across recovery" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 10) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        sub = ch.consume(info.name, no_ack: true)
        ch.confirm_select
        outcome_ch = ::Channel(Amqp::ConfirmOutcome).new(1)
        replay_body = "replayed-pending"
        tag = ch.__spec_register_pending_replay_confirm(
          outcome_ch,
          Amqp::Message.new(replay_body),
          "",
          info.name,
        )
        ch.__spec_pending_recovery_replay_payload_sizes.should eq([replay_body.bytesize])

        conn.__force_disconnect_for_test

        deadline = Time.instant + 10.seconds
        loop do
          break if conn.recovered_to_open?
          fail "connection did not recover within 10s" if Time.instant > deadline
          sleep 100.milliseconds
        end

        outcome = outcome_ch.receive?
        outcome.should_not be_nil
        outcome.not_nil!.kind.ack?.should be_true
        outcome.not_nil!.delivery_tag.should eq(1_u64)
        outcome.not_nil!.delivery_tag.should_not eq(tag) unless tag == 1_u64

        d = sub.receive
        String.new(d.body).should eq(replay_body)
      end
    end

    it "surfaces at-least-once duplicates when the original publish reached the broker before recovery replay" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 10) do |conn|
        ch = conn.open_channel
        queue = "amqp-ng-recovery-duplicate-#{Random::Secure.hex(4)}"
        begin
          info = ch.queue_declare(queue)
          ch.publish("", info.name, "duplicate-risk".to_slice)

          ch.confirm_select
          outcome_ch = ::Channel(Amqp::ConfirmOutcome).new(1)
          ch.__spec_register_pending_replay_confirm(
            outcome_ch,
            Amqp::Message.new("duplicate-risk"),
            "",
            info.name,
          )

          conn.__force_disconnect_for_test

          deadline = Time.instant + 10.seconds
          loop do
            break if conn.recovered_to_open?
            fail "connection did not recover within 10s" if Time.instant > deadline
            sleep 100.milliseconds
          end

          outcome = outcome_ch.receive?
          outcome.should_not be_nil
          outcome.not_nil!.kind.ack?.should be_true

          bodies = [] of String
          2.times do
            msg = ch.get(info.name)
            msg.should_not be_nil
            bodies << String.new(msg.not_nil!.body)
          end
          bodies.sort.should eq(["duplicate-risk", "duplicate-risk"])
        ensure
          ch.queue_delete(queue) rescue nil
        end
      end
    end

    it "re-installs explicit consumer tags across recovery" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 10) do |conn|
        ch = conn.open_channel
        queue = "amqp-ng-consumer-tag-#{Random::Secure.hex(4)}"
        info = ch.queue_declare(queue, auto_delete: true)
        tag = "amqp-ng-explicit-#{Random::Secure.hex(4)}"
        sub = ch.consume(info.name, consumer_tag: tag, no_ack: true)
        sub.consumer_tag.should eq(tag)

        conn.__force_disconnect_for_test

        deadline = Time.instant + 10.seconds
        loop do
          break if conn.recovered_to_open?
          fail "connection did not recover within 10s" if Time.instant > deadline
          sleep 100.milliseconds
        end

        ch.publish("", info.name, "tagged-after-recovery".to_slice)
        delivery = receive_recovery_delivery(sub)
        delivery.consumer_tag.should eq(tag)
        String.new(delivery.body).should eq("tagged-after-recovery")
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

    it "fails new operations with RecoveryInProgress during recovery window" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      # Use a longer initial delay so we have time to observe the Recovering state.
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 2.seconds,
        recovery_max_attempts: 5) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)

        conn.__force_disconnect_for_test

        # Poll until we observe the Recovering state.
        observed_recovering = false
        deadline = Time.instant + 3.seconds
        while Time.instant < deadline
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
        expect_raises(Amqp::RecoveryInProgress) do
          ch.publish("", info.name, "during-recovery".to_slice)
        end
        expect_raises(Amqp::RecoveryInProgress) do
          ch.get(info.name)
        end
        expect_raises(Amqp::RecoveryInProgress) do
          ch.queue_declare("amqp-ng-during-recovery-#{Random::Secure.hex(4)}")
        end

        # Eventually recovery completes.
        deadline = Time.instant + 10.seconds
        loop do
          break if conn.recovered_to_open?
          fail "connection did not recover" if Time.instant > deadline
          sleep 100.milliseconds
        end
      end
    end

    it "does not re-open after caller close during recovery" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 2.seconds,
        recovery_max_attempts: 5) do |conn|
        ch = conn.open_channel
        ch.queue_declare(exclusive: true)

        conn.__force_disconnect_for_test

        deadline = Time.instant + 3.seconds
        until conn.state_recovering?
          fail "connection did not enter recovery" if Time.instant > deadline
          sleep 20.milliseconds
        end

        conn.close
        conn.closed?.should be_true

        sleep 250.milliseconds
        conn.closed?.should be_true
        conn.recovered_to_open?.should be_false
        conn.stats.snapshot.recoveries_succeeded.should eq(0)

        expect_raises(Amqp::SocketError) do
          conn.open_channel
        end
      end
    end

    it "fails closed when topology replay is rejected" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 3) do |conn|
        ch = conn.open_channel
        ch.__spec_record_unreplayable_queue("amq.amqp-ng-recovery-forbidden-#{Random::Secure.hex(4)}")

        conn.__force_disconnect_for_test

        deadline = Time.instant + 10.seconds
        loop do
          break if conn.closed?
          fail "connection did not fail closed after topology replay rejection" if Time.instant > deadline
          sleep 100.milliseconds
        end

        conn.close_reason.should be_a(Amqp::RecoveryExhaustedError)
        conn.stats.snapshot.recoveries_failed.should eq(1)
        ch.closed?.should be_true
      end
    end

    it "fails closed when consumer replay is rejected" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: 100.milliseconds,
        recovery_max_attempts: 3) do |conn|
        ch = conn.open_channel
        missing_queue = "amqp-ng-missing-consumer-#{Random::Secure.hex(4)}"
        ch.__spec_record_unreplayable_consumer(missing_queue, "amqp-ng-missing-consumer-tag")

        conn.__force_disconnect_for_test

        deadline = Time.instant + 10.seconds
        loop do
          break if conn.closed?
          fail "connection did not fail closed after consumer replay rejection" if Time.instant > deadline
          sleep 100.milliseconds
        end

        conn.close_reason.should be_a(Amqp::RecoveryExhaustedError)
        conn.stats.snapshot.recoveries_failed.should eq(1)
        ch.closed?.should be_true
      end
    end

    it "drops recovery topology records when topology and consumers are removed" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, recovery: true) do |conn|
        ch = conn.open_channel
        exchange = "amqp-ng-recovery-records-#{Random::Secure.hex(4)}"
        ch.exchange_declare(exchange, type: "direct", auto_delete: true)
        info = ch.queue_declare(auto_delete: true)
        ch.queue_bind(info.name, exchange, routing_key: "k")
        ch.qos(prefetch_count: 3_u16)
        sub = ch.consume(info.name, no_ack: true)

        ch.__spec_topology_counts.should eq({
          exchanges: 1,
          queues:    1,
          bindings:  1,
          qos:       1,
          consumers: 1,
        })

        sub.close
        ch.queue_unbind(info.name, exchange, routing_key: "k")
        ch.queue_delete(info.name)
        ch.exchange_delete(exchange)

        ch.__spec_topology_counts.should eq({
          exchanges: 0,
          queues:    0,
          bindings:  0,
          qos:       1,
          consumers: 0,
        })
      end
    end
  end
end
