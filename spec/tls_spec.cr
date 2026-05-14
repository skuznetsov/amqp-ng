require "./spec_helper"

describe "Amqp TLS" do
  it "Config.parse infers TLS from amqps:// scheme" do
    cfg = Amqp::Config.parse("amqps://guest:guest@example.com:5671/")
    cfg.tls?.should be_true
    cfg.port.should eq(5671)
    cfg.scheme.should eq("amqps")
  end

  it "Config.parse defaults port 5671 for amqps://" do
    cfg = Amqp::Config.parse("amqps://example.com/")
    cfg.port.should eq(5671)
  end

  it "Config refuses tls_context with non-TLS scheme" do
    ctx = OpenSSL::SSL::Context::Client.new
    expect_raises(Amqp::TlsConfigError) do
      Amqp::Config.parse("amqp://example.com/", tls_context: ctx)
    end
  end

  describe "(live TLS broker via AMQP_TLS_URL)" do
    it "completes handshake against a TLS broker" do
      url = ENV["AMQP_TLS_URL"]?
      pending! "AMQP_TLS_URL not set" unless url

      Amqp.connect(url) do |conn|
        conn.closed?.should be_false
        ch = conn.open_channel
        info = ch.queue_declare(exclusive: true)
        ch.publish("", info.name, "tls-ping".to_slice)
        sub = ch.consume(info.name, no_ack: true)
        d = sub.receive
        String.new(d.body).should eq("tls-ping")
      end
    end
  end
end
