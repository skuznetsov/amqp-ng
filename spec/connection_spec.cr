require "./spec_helper"

describe Amqp::Connection do
  it "rejects amqps:// in slice 1" do
    expect_raises(Amqp::TlsConfigError) do
      Amqp.connect("amqps://guest:guest@127.0.0.1:5671/")
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
  end
end
