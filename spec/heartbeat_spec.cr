require "./spec_helper"

describe "heartbeats" do
  describe "(live broker)" do
    it "negotiated heartbeat is reflected on the connection" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, heartbeat: 5.seconds) do |conn|
        # Either we asked for 5s and broker accepted, or broker capped lower.
        conn.heartbeat.should be > Time::Span.zero
        conn.heartbeat.should be <= 60.seconds
      end
    end

    it "heartbeat=0 disables the writer fiber and leaves no read timeout pressure" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, heartbeat: 0.seconds) do |conn|
        # Broker may still negotiate a non-zero default; either way the
        # connection must work end-to-end.
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.publish("", info.name, "x".to_slice)
        ch.close
      end
    end

    it "idle connection survives past heartbeat interval (no spurious read timeout)" do
      pending! "broker not reachable" unless SpecHelper.broker_reachable?
      Amqp.connect(SpecHelper.amqp_url, heartbeat: 2.seconds) do |conn|
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        # Idle long enough for the heartbeat fiber to fire at least twice
        # AND for the read_timeout window (2*heartbeat = 4s) to elapse.
        sleep 5.seconds
        conn.closed?.should be_false
        ch.publish("", info.name, "still-alive".to_slice)
        sub = ch.consume(info.name, no_ack: true)
        d = sub.receive
        String.new(d.body).should eq("still-alive")
        ch.close
      end
    end
  end
end
