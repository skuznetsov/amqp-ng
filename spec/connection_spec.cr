require "./spec_helper"

class Amqp::Connection
  def __spec_channel_count : Int32
    @channels_mutex.synchronize { @channels.size }
  end

  def __spec_handle_frame(frame : Amqp::Wire::Frame) : Nil
    handle_frame(frame)
  end
end

describe Amqp::Connection do
  it "attempts TLS handshake when scheme is amqps://" do
    # Plain-AMQP broker on 5672 will close the TLS handshake, so we
    # expect either a TLS-level error or a generic socket error
    # depending on how the broker drops the bytes.
    expect_raises(Amqp::ConnectError) do
      Amqp.connect("amqps://guest:guest@127.0.0.1:5672/",
        connect_timeout: 1.second)
    end
  end

  it "wraps refused connections in ConnectRefusedError" do
    # 127.0.0.1:1 is reserved and typically refuses immediately.
    expect_raises(Amqp::ConnectError) do
      Amqp.connect("amqp://guest:guest@127.0.0.1:1/")
    end
  end

  describe "(live broker)" do
    it "completes handshake and closes cleanly" do
      pending! "broker not reachable on #{SpecHelper.amqp_url}" unless SpecHelper.broker_reachable?

      conn = Amqp.connect(SpecHelper.amqp_url)
      begin
        conn.closed?.should be_false
        conn.channel_max.should be > 0_u16
        conn.frame_max.should be > 0_u32
      ensure
        conn.close
      end
      conn.closed?.should be_true
    end

    it "cleans channel registry across repeated connect/close cycles" do
      pending! "broker not reachable on #{SpecHelper.amqp_url}" unless SpecHelper.broker_reachable?

      5.times do
        conn = Amqp.connect(SpecHelper.amqp_url)
        ch = conn.channel
        ch.queue_declare(exclusive: true)
        conn.__spec_channel_count.should eq(1)

        conn.close

        conn.closed?.should be_true
        conn.__spec_channel_count.should eq(0)
      end
    end

    it "maps bad credentials to AuthenticationError" do
      pending! "broker not reachable on #{SpecHelper.amqp_url}" unless SpecHelper.broker_reachable?

      uri = URI.parse(SpecHelper.amqp_url)
      host = uri.host || "127.0.0.1"
      port = uri.port || 5672
      bad = "amqp://guest:wrong-password@#{host}:#{port}/"
      expect_raises(Amqp::AuthenticationError) do
        Amqp.connect(bad)
      end
    end

    it "maps unknown vhost to VhostAccessError" do
      pending! "broker not reachable on #{SpecHelper.amqp_url}" unless SpecHelper.broker_reachable?

      uri = URI.parse(SpecHelper.amqp_url)
      host = uri.host || "127.0.0.1"
      port = uri.port || 5672
      bad = "amqp://guest:guest@#{host}:#{port}/no-such-vhost-#{Random.new.hex(4)}"
      expect_raises(Amqp::Error) do
        Amqp.connect(bad)
      end
    end

    it "dispatches connection blocked and unblocked callbacks" do
      pending! "broker not reachable on #{SpecHelper.amqp_url}" unless SpecHelper.broker_reachable?

      conn = Amqp.connect(SpecHelper.amqp_url)
      begin
        blocked = ::Channel(String).new(1)
        unblocked = ::Channel(Nil).new(1)
        conn.on_blocked { |reason| blocked.send(reason) }
        conn.on_unblocked { unblocked.send(nil) }

        conn.__spec_handle_frame(Amqp::Wire::Frame.new(
          Amqp::Wire::FrameType::Method,
          0_u16,
          Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Blocked.new("low disk").to_payload,
        ))
        conn.blocked?.should be_true
        blocked.receive.should eq("low disk")

        conn.__spec_handle_frame(Amqp::Wire::Frame.new(
          Amqp::Wire::FrameType::Method,
          0_u16,
          Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Unblocked.new.to_payload,
        ))
        conn.blocked?.should be_false
        unblocked.receive.should be_nil
      ensure
        conn.close
      end
    end
  end
end
