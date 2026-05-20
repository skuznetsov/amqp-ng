require "./spec_helper"

describe Amqp::Config do
  it "parses URI defaults and vhost forms" do
    Amqp::Config.parse("amqp://example.com").vhost.should eq("/")
    Amqp::Config.parse("amqp://example.com/").vhost.should eq("/")
    Amqp::Config.parse("amqp://example.com/app").vhost.should eq("app")
    Amqp::Config.parse("amqp://example.com/app%2Fweb").vhost.should eq("app/web")
    Amqp::Config.parse("amqp://example.com/%2F").vhost.should eq("/")
    Amqp::Config.parse("amqp://example.com/app", vhost: "").vhost.should eq("")
    Amqp::Config.parse("amqps://example.com").port.should eq(5671)
    Amqp::Config.parse("amqp://example.com").port.should eq(5672)
  end

  it "parses supported query keys" do
    cfg = Amqp::Config.parse(
      "amqp://user:p%40ss@example.com/app?heartbeat=7&channel_max=9&frame_max=4096&max_body_size=1048576&connect_timeout=3&tcp_nodelay=true&buffer_size=32768&recovery=full&product=p&information=i"
    )
    cfg.user.should eq("user")
    cfg.password.should eq("p@ss")
    cfg.heartbeat.should eq(7.seconds)
    cfg.channel_max.should eq(9_u16)
    cfg.frame_max.should eq(4096_u32)
    cfg.max_body_size.should eq(1_048_576_u64)
    cfg.connect_timeout.should eq(3.seconds)
    cfg.tcp_nodelay?.should be_true
    cfg.buffer_size.should eq(32768)
    cfg.recovery?.should be_true
    cfg.product.should eq("p")
    cfg.information.should eq("i")
  end

  it "lets keyword arguments override URI query values" do
    cfg = Amqp::Config.parse(
      "amqp://u:p@example.com/q?heartbeat=7&channel_max=9&frame_max=4096&max_body_size=1048576&connect_timeout=3&tcp_nodelay=true&buffer_size=32768&recovery=full&product=query&information=query",
      user: "kw-user",
      password: "kw-pass",
      vhost: "kw-vhost",
      heartbeat: 8.seconds,
      channel_max: 10_u16,
      frame_max: 8192_u32,
      max_body_size: 2_097_152_u64,
      connect_timeout: 4.seconds,
      tcp_nodelay: false,
      buffer_size: 0,
      recovery: Amqp::Recovery::None,
      product: "kw-product",
      information: "kw-information"
    )
    cfg.user.should eq("kw-user")
    cfg.password.should eq("kw-pass")
    cfg.vhost.should eq("kw-vhost")
    cfg.heartbeat.should eq(8.seconds)
    cfg.channel_max.should eq(10_u16)
    cfg.frame_max.should eq(8192_u32)
    cfg.max_body_size.should eq(2_097_152_u64)
    cfg.connect_timeout.should eq(4.seconds)
    cfg.tcp_nodelay?.should be_false
    cfg.buffer_size.should eq(0)
    cfg.recovery?.should be_false
    cfg.product.should eq("kw-product")
    cfg.information.should eq("kw-information")
  end

  it "rejects unknown query keys with the offending key" do
    err = expect_raises(Amqp::UriError) do
      Amqp::Config.parse("amqp://example.com?heartbets=30")
    end
    err.message.not_nil!.should contain("heartbets")
  end

  it "rejects deferred TLS and SASL query keys as unknown" do
    ["verify", "cacertfile", "certfile", "keyfile", "server_name", "auth_mechanism"].each do |key|
      err = expect_raises(Amqp::UriError) do
        Amqp::Config.parse("amqps://example.com?#{key}=x")
      end
      err.message.not_nil!.should contain(key)
    end
  end

  it "rejects tls_context with plain amqp scheme" do
    ctx = OpenSSL::SSL::Context::Client.new
    expect_raises(Amqp::TlsConfigError) do
      Amqp::Config.parse("amqp://example.com", tls_context: ctx)
    end
  end

  it "rejects unsupported schemes and missing hosts" do
    expect_raises(Amqp::UriError) { Amqp::Config.parse("http://example.com") }
    expect_raises(Amqp::UriError) { Amqp::Config.parse("amqp:///") }
  end

  it "decodes percent-encoded reserved userinfo characters" do
    cfg = Amqp::Config.parse("amqp://u%40ser:p%3Aa%2Fss@example.com")
    cfg.user.should eq("u@ser")
    cfg.password.should eq("p:a/ss")
  end

  it "rejects non-decimal numeric query values" do
    ["30s", "+30", "30_000"].each do |value|
      expect_raises(Amqp::UriError) do
        Amqp::Config.parse("amqp://example.com?heartbeat=#{value}")
      end
    end
  end

  it "rejects invalid socket tuning query values" do
    expect_raises(Amqp::UriError) { Amqp::Config.parse("amqp://example.com?tcp_nodelay=1") }
    expect_raises(Amqp::UriError) { Amqp::Config.parse("amqp://example.com?buffer_size=-1") }
    expect_raises(Amqp::UriError) { Amqp::Config.parse("amqp://example.com?buffer_size=#{Int64::MAX}") }
  end

  it "rejects invalid max body size values" do
    expect_raises(Amqp::UriError) { Amqp::Config.parse("amqp://example.com?max_body_size=0") }
    expect_raises(Amqp::UriError) { Amqp::Config.parse("amqp://example.com?max_body_size=-1") }
    expect_raises(Amqp::UriError) { Amqp::Config.parse("amqp://example.com?max_body_size=#{UInt64::MAX}0") }
    expect_raises(Amqp::ConfigurationError) { Amqp::Config.parse("amqp://example.com", max_body_size: 0_u64) }
  end

  it "rejects invalid recovery query values" do
    expect_raises(Amqp::UriError) do
      Amqp::Config.parse("amqp://example.com?recovery=true")
    end
  end
end
