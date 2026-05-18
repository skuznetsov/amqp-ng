# TCP capture proxy for AMQP corpus.
#
# Usage:
#   crystal run tools/capture_proxy.cr -- <scenario-name> [listen-port] [upstream-host] [upstream-port]
#
# Defaults: listen 127.0.0.1:5673, upstream 127.0.0.1:5672.
#
# Behaviour:
#   Accepts the FIRST client connection, opens an upstream socket, forwards bytes
#   in both directions, and writes everything to:
#     spec/fixtures/frames/<scenario>/c2s.bin
#     spec/fixtures/frames/<scenario>/s2c.bin
#   plus a transcript `meta.txt` with start/end timestamps and byte counts.
#
#   After the client OR upstream half-closes, the proxy drains the other side
#   for up to 200ms then exits. One scenario = one process invocation.

require "socket"
require "file_utils"

scenario = ARGV[0]? || abort "usage: capture_proxy <scenario> [port] [upstream-host] [upstream-port]"
listen_port = (ARGV[1]? || "5673").to_i
upstream_host = ARGV[2]? || "127.0.0.1"
upstream_port = (ARGV[3]? || "5672").to_i

root = File.expand_path("../spec/fixtures/frames/#{scenario}", __DIR__)
FileUtils.mkdir_p(root)

c2s_path = File.join(root, "c2s.bin")
s2c_path = File.join(root, "s2c.bin")
meta_path = File.join(root, "meta.txt")

c2s_file = File.open(c2s_path, "wb")
s2c_file = File.open(s2c_path, "wb")

server = TCPServer.new("127.0.0.1", listen_port)
STDERR.puts "[capture] scenario=#{scenario} listening 127.0.0.1:#{listen_port} -> #{upstream_host}:#{upstream_port}"
STDOUT.puts "READY"
STDOUT.flush

t0 = Time.instant
client = server.accept
server.close # only one connection per scenario
upstream = TCPSocket.new(upstream_host, upstream_port)
STDERR.puts "[capture] client connected, upstream open"

c2s_bytes = Atomic(Int64).new(0_i64)
s2c_bytes = Atomic(Int64).new(0_i64)
done = Channel(Symbol).new(2)

spawn do
  buf = Bytes.new(16 * 1024)
  loop do
    n = client.read(buf)
    break if n == 0
    chunk = buf[0, n]
    upstream.write(chunk)
    upstream.flush
    c2s_file.write(chunk)
    c2s_file.flush
    c2s_bytes.add(n.to_i64)
  end
rescue ex
  STDERR.puts "[capture] c2s ended: #{ex.message}"
ensure
  done.send(:c2s) rescue nil
end

spawn do
  buf = Bytes.new(16 * 1024)
  loop do
    n = upstream.read(buf)
    break if n == 0
    chunk = buf[0, n]
    client.write(chunk)
    client.flush
    s2c_file.write(chunk)
    s2c_file.flush
    s2c_bytes.add(n.to_i64)
  end
rescue ex
  STDERR.puts "[capture] s2c ended: #{ex.message}"
ensure
  done.send(:s2c) rescue nil
end

# Wait for first half to finish, then drain other side for 200ms.
first = done.receive
STDERR.puts "[capture] half-closed: #{first}; draining..."
select
when done.receive
when timeout(200.milliseconds)
end

t1 = Time.instant
client.close rescue nil
upstream.close rescue nil
c2s_file.close
s2c_file.close

File.write(meta_path, String.build { |io|
  io << "scenario: " << scenario << '\n'
  io << "captured_at: " << Time.utc.to_rfc3339 << '\n'
  io << "duration_ms: " << (t1 - t0).total_milliseconds.to_i64 << '\n'
  io << "c2s_bytes: " << c2s_bytes.get << '\n'
  io << "s2c_bytes: " << s2c_bytes.get << '\n'
  io << "upstream: " << upstream_host << ':' << upstream_port << '\n'
})

STDERR.puts "[capture] done c2s=#{c2s_bytes.get} s2c=#{s2c_bytes.get} -> #{root}"
