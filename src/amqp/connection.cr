require "log"
require "socket"
require "openssl"
require "./error"
require "./config"
require "./stats"
require "./arguments"
require "./properties"
require "./wire/protocol_header"
require "./wire/frame"
require "./wire/amqp_zero_nine_one/types"
require "./wire/amqp_zero_nine_one/connection_methods"
require "./wire/amqp_zero_nine_one/channel_methods"
require "./wire/amqp_zero_nine_one/exchange_methods"
require "./wire/amqp_zero_nine_one/queue_methods"
require "./wire/amqp_zero_nine_one/basic_methods"
require "./wire/amqp_zero_nine_one/content_header"
require "./channel"

module Amqp
  VERSION = "0.1.0"

  class Connection
    Log = ::Log.for("amqp.connection")

    enum State
      Initial
      Connecting
      Negotiating
      Open
      Recovering
      Closing
      Closed
    end

    getter config : Config
    getter channel_max : UInt16
    getter frame_max : UInt32
    getter heartbeat : UInt16
    getter? closed : Bool
    getter? blocked : Bool

    def state_recovering? : Bool
      @state == State::Recovering
    end

    def recovered_to_open? : Bool
      @state == State::Open
    end
    @state : State
    @socket : TCPSocket?
    @io : IO?
    @write_mutex : Mutex
    @channels : Hash(UInt16, Channel)
    @channels_mutex : Mutex
    @next_channel_id : UInt16
    @reader_done : ::Channel(Nil)
    @heartbeat_done : ::Channel(Nil)
    @heartbeat_stop : ::Channel(Nil)
    @last_write_ns : Atomic(Int64)
    @close_reason : Exception?
    getter stats : Stats

    private def initialize(@config : Config)
      @state = State::Initial
      @channel_max = 0_u16
      @frame_max = 0_u32
      @heartbeat = 0_u16
      @closed = false
      @blocked = false
      @write_mutex = Mutex.new
      @channels = {} of UInt16 => Channel
      @channels_mutex = Mutex.new
      @next_channel_id = 1_u16
      @reader_done = ::Channel(Nil).new
      @heartbeat_done = ::Channel(Nil).new
      @heartbeat_stop = ::Channel(Nil).new
      @last_write_ns = Atomic(Int64).new(0_i64)
      @close_reason = nil
      @stats = Stats.new
    end

    private def monotonic_ns : Int64
      Time.monotonic.total_nanoseconds.to_i64
    end

    private def stamp_write : Nil
      @last_write_ns.set(monotonic_ns)
    end

    def self.connect(config : Config) : Connection
      conn = new(config)
      conn.start
      conn
    end

    protected def start : Nil
      begin
        establish_session
      rescue ex
        @state = State::Closed
        @closed = true
        raise ex
      end
      @state = State::Open
      start_session_fibers
      Log.info { "connection open #{@config.scheme}://#{@config.host}:#{@config.port}#{@config.vhost} (heartbeat=#{@heartbeat}s, channel_max=#{@channel_max}, frame_max=#{@frame_max})" }
    end

    # Open TCP socket and run the AMQP handshake. Sets @socket / @io and
    # applies the heartbeat-derived read_timeout. Raises a typed
    # ConnectError or SocketError on failure.
    private def establish_session : Nil
      @state = State::Connecting
      sock = begin
        TCPSocket.new(@config.host, @config.port,
          connect_timeout: @config.connect_timeout)
      rescue ex : IO::TimeoutError
        raise ConnectTimeoutError.new("connect to #{@config.host}:#{@config.port} timed out", ex)
      rescue ex : Socket::ConnectError
        raise ConnectRefusedError.new("connect to #{@config.host}:#{@config.port} refused: #{ex.message}", ex)
      rescue ex : IO::Error | Socket::Error
        raise SocketError.new("socket error: #{ex.message}", ex)
      end

      sock.sync = false
      sock.read_buffering = true
      sock.tcp_nodelay = true
      @socket = sock

      io : IO = sock
      if @config.tls?
        begin
          ctx = @config.tls_context || default_tls_context
          io = OpenSSL::SSL::Socket::Client.new(sock, context: ctx,
            sync_close: true, hostname: @config.host)
        rescue ex : OpenSSL::SSL::Error
          begin
            sock.close
          rescue
          end
          raise TlsHandshakeError.new("TLS handshake to #{@config.host}:#{@config.port} failed: #{ex.message}", ex)
        rescue ex : IO::Error | Socket::Error
          begin
            sock.close
          rescue
          end
          raise SocketError.new("socket error during TLS handshake: #{ex.message}", ex)
        end
      end
      @io = io

      @state = State::Negotiating
      begin
        perform_handshake(io)
      rescue ex
        begin
          io.close
        rescue
        end
        raise ex
      end

      if @heartbeat > 0
        sock.read_timeout = (@heartbeat.to_i * 2).seconds
      end
      stamp_write
    end

    private def default_tls_context : OpenSSL::SSL::Context::Client
      OpenSSL::SSL::Context::Client.new
    end

    private def start_session_fibers : Nil
      @reader_done = ::Channel(Nil).new
      @heartbeat_done = ::Channel(Nil).new
      @heartbeat_stop = ::Channel(Nil).new
      spawn name: "amqp-reader-#{@config.host}:#{@config.port}" do
        run_reader_loop
      end
      if @heartbeat > 0
        spawn name: "amqp-heartbeat-#{@config.host}:#{@config.port}" do
          run_heartbeat_loop
        end
      else
        @heartbeat_done.close rescue nil
      end
    end

    # ---- Handshake ------------------------------------------------------

    private def perform_handshake(io : IO) : Nil
      io.write(Amqp::Wire::PROTOCOL_HEADER)
      io.flush

      start = expect_connection_method(io, Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_START) do |body_io|
        Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Start.read(body_io)
      end
      assert_mechanism_supported(start.mechanisms)

      response_bytes = build_plain_response(@config.user, @config.password)
      start_ok = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::StartOk.new(
        client_properties: client_properties,
        mechanism: "PLAIN",
        response: response_bytes,
        locale: "en_US",
      )
      write_method_frame(io, 0_u16, start_ok.to_payload)

      tune = expect_connection_method(io, Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_TUNE) do |body_io|
        Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Tune.read(body_io)
      end

      @channel_max = negotiate_short(tune.channel_max, @config.channel_max)
      @frame_max = negotiate_long(tune.frame_max, @config.frame_max)
      @heartbeat = negotiate_short(tune.heartbeat, @config.heartbeat.total_seconds.to_u16)

      tune_ok = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::TuneOk.new(
        channel_max: @channel_max,
        frame_max: @frame_max,
        heartbeat: @heartbeat,
      )
      write_method_frame(io, 0_u16, tune_ok.to_payload)

      open = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Open.new(@config.vhost)
      write_method_frame(io, 0_u16, open.to_payload)

      expect_connection_method(io, Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_OPEN_OK) do |body_io|
        Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::OpenOk.read(body_io)
      end
    end

    private def assert_mechanism_supported(mechanisms : String) : Nil
      list = mechanisms.split(' ').reject(&.empty?)
      unless list.includes?("PLAIN")
        raise ProtocolNegotiationError.new("broker offers #{mechanisms.inspect}; PLAIN required by v0")
      end
    end

    private def build_plain_response(user : String, password : String) : Bytes
      buf = IO::Memory.new
      buf.write_byte(0_u8)
      buf.write(user.to_slice)
      buf.write_byte(0_u8)
      buf.write(password.to_slice)
      buf.to_slice
    end

    private def client_properties : Amqp::Arguments
      caps = Amqp::Arguments{
        "authentication_failure_close" => true.as(Amqp::FieldValue),
        "connection.blocked"           => true.as(Amqp::FieldValue),
        "consumer_cancel_notify"       => true.as(Amqp::FieldValue),
        "publisher_confirms"           => true.as(Amqp::FieldValue),
      }
      info = @config.information || "amqp-ng Crystal client"
      Amqp::Arguments{
        "product"      => @config.product.as(Amqp::FieldValue),
        "version"      => VERSION.as(Amqp::FieldValue),
        "platform"     => "Crystal #{Crystal::VERSION}".as(Amqp::FieldValue),
        "information"  => info.as(Amqp::FieldValue),
        "capabilities" => caps.as(Amqp::FieldValue),
      }
    end

    private def negotiate_short(server : UInt16, client : UInt16) : UInt16
      return client if server == 0
      return server if client == 0
      server < client ? server : client
    end

    private def negotiate_long(server : UInt32, client : UInt32) : UInt32
      return client if server == 0
      return server if client == 0
      server < client ? server : client
    end

    private def expect_connection_method(io : IO, expected_method_id : UInt16, & : IO -> T) : T forall T
      cap = @frame_max == 0 ? 131_072_u32 : @frame_max
      frame = Amqp::Wire::Frame.read(io, cap)

      unless frame.type.method?
        raise ProtocolError.new("expected method frame, got #{frame.type}")
      end
      unless frame.channel == 0
        raise ProtocolError.new("expected channel 0 during handshake, got #{frame.channel}")
      end

      body = IO::Memory.new(frame.payload, false)
      class_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
      method_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)

      unless class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CONNECTION
        raise ProtocolError.new("expected connection class (10), got #{class_id}")
      end

      if method_id == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_CLOSE
        close = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Close.read(body)
        begin
          close_ok = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::CloseOk.new
          write_method_frame(io, 0_u16, close_ok.to_payload)
        rescue
        end
        raise map_broker_close(close)
      end

      unless method_id == expected_method_id
        raise ProtocolError.new("expected method #{expected_method_id}, got #{method_id}")
      end

      yield body
    end

    private def map_broker_close(close : Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Close) : Exception
      case close.reply_code
      when 403_u16
        text = close.reply_text.downcase
        if text.includes?("vhost") || text.includes?("virtual host")
          VhostAccessError.new("vhost access refused: #{close.reply_text}")
        else
          AuthenticationError.new("authentication failed: #{close.reply_text}")
        end
      when 530_u16
        VhostAccessError.new("vhost access refused (530): #{close.reply_text}")
      else
        ConnectionClosedByBroker.new(
          close.reply_code, close.reply_text, close.class_id, close.method_id,
        )
      end
    end

    # ---- Frame I/O ------------------------------------------------------

    # Writes one method frame under the connection write-mutex.
    protected def write_method_frame(io : IO, channel : UInt16, payload : Bytes) : Nil
      @write_mutex.synchronize do
        Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, channel, payload).write(io)
        io.flush
        stamp_write
      end
    end

    # For multi-frame sequences (publish = method + header + body...), the
    # caller must hold the write mutex across the entire sequence so other
    # writers cannot interleave. This yields the IO under the lock.
    protected def with_write(& : IO -> _) : Nil
      io = @io || raise SocketError.new("connection closed")
      @write_mutex.synchronize do
        yield io
        io.flush
        stamp_write
      end
    end

    protected def write_frame(channel : UInt16, type : Amqp::Wire::FrameType, payload : Bytes) : Nil
      io = @io || raise SocketError.new("connection closed")
      @write_mutex.synchronize do
        Amqp::Wire::Frame.new(type, channel, payload).write(io)
        io.flush
        stamp_write
      end
    end

    # ---- Channel registry ----------------------------------------------

    def open_channel : Channel
      raise SocketError.new("connection closed") if @closed
      raise RecoveryInProgress.new("connection is recovering") if @state == State::Recovering
      id = @channels_mutex.synchronize do
        cap = @channel_max == 0 ? UInt16::MAX : @channel_max
        scanned = 0
        loop do
          candidate = @next_channel_id
          @next_channel_id = (candidate == cap) ? 1_u16 : (candidate + 1_u16)
          unless @channels.has_key?(candidate)
            ch = Channel.new(self, candidate)
            @channels[candidate] = ch
            break candidate
          end
          scanned += 1
          raise ChannelLimitError.new("no free channel id (channel_max=#{cap})") if scanned > cap.to_i
        end
      end
      channel = @channels[id]
      channel.open
      channel
    end

    protected def unregister_channel(id : UInt16) : Nil
      @channels_mutex.synchronize { @channels.delete(id) }
    end

    protected def lookup_channel(id : UInt16) : Channel?
      @channels_mutex.synchronize { @channels[id]? }
    end

    # ---- Reader loop ----------------------------------------------------

    private def run_reader_loop : Nil
      io = @io.not_nil!
      cap = @frame_max == 0 ? 131_072_u32 : @frame_max
      loop do
        break if @closed
        frame = Amqp::Wire::Frame.read(io, cap)
        handle_frame(frame)
      end
    rescue ex : IO::TimeoutError
      handle_session_loss(HeartbeatTimeoutError.new(
        "no frame received within heartbeat window (#{@heartbeat}s)", ex))
    rescue ex : IO::EOFError
      handle_session_loss(SocketError.new("connection EOF", ex))
    rescue ex : Amqp::Error
      handle_session_loss(ex)
    rescue ex : IO::Error
      handle_session_loss(SocketError.new("socket error: #{ex.message}", ex))
    ensure
      @reader_done.close rescue nil
    end

    # Choose between full shutdown and an automatic recovery cycle.
    # Recovery is opt-in via Config#recovery and only fires for transport
    # failures — deliberate broker close, auth, and vhost errors always
    # tear the connection down.
    private def handle_session_loss(reason : Exception) : Nil
      if @config.recovery? && recoverable?(reason) && @state != State::Closing && @state != State::Closed
        spawn(name: "amqp-recovery-#{@config.host}:#{@config.port}") do
          attempt_recovery(reason)
        end
      else
        shutdown_with(reason)
      end
    end

    private def recoverable?(ex : Exception) : Bool
      case ex
      when HeartbeatTimeoutError, SocketError
        true
      else
        false
      end
    end

    private def run_heartbeat_loop : Nil
      interval_ns = @heartbeat.to_i64 * 1_000_000_000_i64
      tick = (@heartbeat.to_f64 / 2.0).seconds
      tick = 1.seconds if tick < 1.seconds
      loop do
        select
        when @heartbeat_stop.receive?
          break
        when timeout(tick)
        end
        break if @closed
        idle_ns = monotonic_ns - @last_write_ns.get
        if idle_ns >= interval_ns
          begin
            write_frame(0_u16, Amqp::Wire::FrameType::Heartbeat, Bytes.empty)
          rescue
            # write failed → reader will surface it; nothing useful here.
            break
          end
        end
      end
    rescue
      # swallow; shutdown drives everything
    ensure
      @heartbeat_done.close rescue nil
    end

    private def handle_frame(frame : Amqp::Wire::Frame) : Nil
      if frame.channel == 0
        handle_connection_frame(frame)
      else
        ch = lookup_channel(frame.channel)
        if ch
          ch.dispatch_frame(frame)
        else
          # Unknown channel — could be a late frame after close. Ignore.
        end
      end
    end

    private def handle_connection_frame(frame : Amqp::Wire::Frame) : Nil
      case frame.type
      when .heartbeat?
        # No-op for slice 2 (heartbeats are slice 4); just accept.
      when .method?
        body = IO::Memory.new(frame.payload, false)
        class_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        method_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        unless class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CONNECTION
          raise ProtocolError.new("non-connection method on channel 0: class=#{class_id}")
        end
        case method_id
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_CLOSE
          close = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Close.read(body)
          begin
            close_ok = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::CloseOk.new
            io = @io
            write_method_frame(io, 0_u16, close_ok.to_payload) if io
          rescue
          end
          raise map_broker_close(close)
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_CLOSE_OK
          # Response to our caller-initiated close; close() already drains.
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_BLOCKED
          @blocked = true
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONNECTION_UNBLOCKED
          @blocked = false
        else
          raise ProtocolError.new("unexpected connection method on channel 0: #{method_id}")
        end
      else
        raise ProtocolError.new("unexpected frame type on channel 0: #{frame.type}")
      end
    end

    # Recovery driver. Runs on its own fiber, spawned by the reader once
    # a recoverable transport error is detected. Suspends every channel,
    # tears down old socket/heartbeat, retries the handshake with
    # exponential backoff, then replays per-channel topology. On
    # exhaustion or a non-retryable handshake failure, falls back to
    # shutdown_with so callers see the final error.
    private def attempt_recovery(reason : Exception) : Nil
      @state = State::Recovering
      @stats.incr_recoveries_attempted
      Log.warn { "recovery starting (reason: #{reason.class}: #{reason.message})" }
      stop_heartbeat

      channels = @channels_mutex.synchronize { @channels.values.dup }
      channels.each(&.enter_recovery(reason))

      if sock = @socket
        begin
          sock.close
        rescue
        end
      end

      delay = @config.recovery_initial_delay
      max_delay = @config.recovery_max_delay
      max_attempts = @config.recovery_max_attempts
      last_error : Exception = reason
      attempt = 0
      established = false
      loop do
        attempt += 1
        sleep delay
        break if @state == State::Closing || @state == State::Closed
        begin
          establish_session
          established = true
          Log.info { "recovery handshake ok on attempt #{attempt}" }
          break
        rescue ex : AuthenticationError | VhostAccessError | TlsConfigError | TlsHandshakeError | ProtocolNegotiationError | ConnectionClosedByBroker
          Log.error { "recovery aborted (non-retryable #{ex.class}): #{ex.message}" }
          last_error = ex
          break
        rescue ex
          Log.warn { "recovery attempt #{attempt}/#{max_attempts} failed (#{ex.class}): #{ex.message}" }
          last_error = ex
          if attempt >= max_attempts
            break
          end
          new_delay = delay * 2
          delay = new_delay > max_delay ? max_delay : new_delay
        end
      end

      unless established
        @stats.incr_recoveries_failed
        Log.error { "recovery exhausted after #{attempt} attempts" }
        shutdown_with(RecoveryExhaustedError.new(
          "recovery failed after #{attempt} attempt(s): #{last_error.message}", last_error
        ))
        return
      end

      start_session_fibers

      begin
        channels.each(&.replay_topology)
      rescue ex
        @stats.incr_recoveries_failed
        Log.error { "topology replay failed: #{ex.class}: #{ex.message}" }
        shutdown_with(RecoveryExhaustedError.new(
          "topology replay failed: #{ex.message}", ex
        ))
        return
      end

      @state = State::Open
      @stats.incr_recoveries_succeeded
      Log.info { "recovery complete (replayed #{channels.size} channel(s))" }
    end

    private def shutdown_with(reason : Exception) : Nil
      @close_reason ||= reason
      @state = State::Closed
      @closed = true
      stop_heartbeat
      chans = @channels_mutex.synchronize do
        list = @channels.values
        @channels.clear
        list
      end
      chans.each(&.abort_with(reason))
      if sock = @socket
        begin
          sock.close
        rescue
        end
      end
    end

    private def stop_heartbeat : Nil
      select
      when @heartbeat_stop.send(nil)
      else
        # fiber not waiting / already stopping
      end
    end

    # :nodoc:
    # Test-only hook: simulates an abrupt socket loss so recovery specs
    # can exercise the reconnect path without needing to bounce the broker.
    # Flips state to Recovering synchronously so callers polling
    # `recovered_to_open?` immediately after this returns don't observe
    # the brief Open window before the reader fiber notices the close.
    def __force_disconnect_for_test : Nil
      @state = State::Recovering if @state == State::Open
      if sock = @socket
        sock.close rescue nil
      end
    end

    # ---- Caller close ---------------------------------------------------

    def close : Nil
      return if @closed
      @state = State::Closing
      sock = @socket
      io = @io
      reason = ChannelClosedByCaller.new("connection closed by caller")

      stop_heartbeat

      chans = @channels_mutex.synchronize do
        list = @channels.values
        @channels.clear
        list
      end
      chans.each(&.abort_with(reason))

      if io
        begin
          close_frame = Amqp::Wire::AmqpZeroNineOne::ConnectionMethods::Close.new(
            reply_code: 200_u16, reply_text: "OK", class_id: 0_u16, method_id: 0_u16,
          )
          write_method_frame(io, 0_u16, close_frame.to_payload)
        rescue
        end
      end

      @closed = true
      @state = State::Closed

      if sock
        begin
          sock.close
        rescue
        end
      end

      # Wait for the reader fiber to drain so we don't leak it.
      begin
        select
        when @reader_done.receive?
          # ok
        when timeout(2.seconds)
          # give up
        end
      rescue
      end
    end
  end
end
