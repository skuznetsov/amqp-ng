require "./spec_helper"
require "file_utils"

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

  it "rejects a trusted TLS certificate whose SAN does not match the URL host" do
    fixture = tls_self_signed_fixture("wrong-host.test")
    begin
      TCPServer.open("127.0.0.1", 0) do |server|
        server_context = OpenSSL::SSL::Context::Server.new
        server_context.certificate_chain = fixture[:cert]
        server_context.private_key = fixture[:key]
        server_done = Channel(Exception?).new
        spawn_tls_fixture_server(server, server_context, server_done)

        client_context = Amqp.tls_context_default
        client_context.ca_certificates = fixture[:cert]

        expect_raises(Amqp::TlsHandshakeError) do
          Amqp.connect("amqps://guest:guest@127.0.0.1:#{server.local_address.port}/",
            tls_context: client_context)
        end

        assert_tls_fixture_server_done(server_done)
      end
    ensure
      FileUtils.rm_rf(fixture[:dir])
    end
  rescue ex : File::NotFoundError
    pending! "openssl CLI not available: #{ex.message}"
  end

  describe "(live TLS broker via AMQP_TLS_URL)" do
    it "completes handshake against a TLS broker" do
      url = ENV["AMQP_TLS_URL"]?
      pending! "AMQP_TLS_URL not set" unless url
      tls_context = if ca_cert = ENV["AMQP_TLS_CA_CERT"]?
                      ctx = Amqp.tls_context_default
                      ctx.ca_certificates = ca_cert
                      ctx
                    end

      Amqp.connect(url, tls_context: tls_context) do |conn|
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

private def tls_self_signed_fixture(hostname : String)
  dir = File.tempname("amqp-ng-tls", nil)
  Dir.mkdir(dir)
  cert = File.join(dir, "cert.pem")
  key = File.join(dir, "key.pem")
  status = Process.run("openssl", [
    "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1",
    "-nodes", "-keyout", key, "-out", cert,
    "-subj", "/CN=#{hostname}",
    "-addext", "subjectAltName=DNS:#{hostname}",
  ], output: Process::Redirect::Close, error: Process::Redirect::Close)
  raise "openssl failed to generate TLS fixture" unless status.success?
  {dir: dir, cert: cert, key: key}
rescue ex
  FileUtils.rm_rf(dir) if dir
  raise ex
end

private def spawn_tls_fixture_server(server : TCPServer,
                                     context : OpenSSL::SSL::Context::Server,
                                     done : Channel(Exception?)) : Nil
  spawn do
    begin
      socket = server.accept
      begin
        ssl = OpenSSL::SSL::Socket::Server.new(socket, context, sync_close: true)
        ssl.close
      rescue OpenSSL::SSL::Error | IO::Error
        # Expected when the client rejects the certificate during handshake.
      ensure
        socket.close rescue nil
      end
      done.send(nil)
    rescue ex
      done.send(ex)
    end
  end
end

private def assert_tls_fixture_server_done(done : Channel(Exception?)) : Nil
  select
  when ex = done.receive
    raise ex if ex
  when timeout(2.seconds)
    raise "TLS fixture server did not finish"
  end
end
