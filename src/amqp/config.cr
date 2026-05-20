require "uri"
require "openssl"
require "./error"
require "./message"

module Amqp
  struct Config
    RECOGNIZED_QUERY_KEYS = {
      "heartbeat",
      "channel_max",
      "frame_max",
      "max_body_size",
      "connect_timeout",
      "tcp_nodelay",
      "buffer_size",
      "recovery",
      "product",
      "information",
    }

    getter scheme : String
    getter host : String
    getter port : Int32
    getter user : String
    getter password : String
    getter vhost : String
    getter heartbeat : Time::Span
    getter channel_max : UInt16
    getter frame_max : UInt32
    getter max_body_size : UInt64
    getter connect_timeout : Time::Span
    getter? tcp_nodelay : Bool
    getter buffer_size : Int32
    getter product : String
    getter information : String?
    getter? recovery : Bool
    getter recovery_max_attempts : Int32
    getter recovery_initial_delay : Time::Span
    getter recovery_max_delay : Time::Span
    getter tls_context : OpenSSL::SSL::Context::Client?

    def initialize(@scheme, @host, @port, @user, @password, @vhost,
                   @heartbeat, @channel_max, @frame_max, @max_body_size,
                   @connect_timeout, @tcp_nodelay, @buffer_size, @product, @information,
                   @recovery = false,
                   @recovery_max_attempts = 5,
                   @recovery_initial_delay = 500.milliseconds,
                   @recovery_max_delay = 10.seconds,
                   @tls_context : OpenSSL::SSL::Context::Client? = nil)
      if @tls_context && @scheme != "amqps"
        raise TlsConfigError.new("tls_context provided but scheme is '#{@scheme}', expected 'amqps'")
      end
      if @max_body_size == 0
        raise ConfigurationError.new("max_body_size must be positive")
      end
    end

    def tls? : Bool
      @scheme == "amqps"
    end

    def self.parse(uri_str : String,
                   *,
                   user : String? = nil,
                   password : String? = nil,
                   vhost : String? = nil,
                   heartbeat : Time::Span? = nil,
                   channel_max : UInt16? = nil,
                   frame_max : UInt32? = nil,
                   max_body_size : UInt64? = nil,
                   connect_timeout : Time::Span? = nil,
                   tcp_nodelay : Bool? = nil,
                   buffer_size : Int32? = nil,
                   product : String? = nil,
                   information : String? = nil,
                   recovery : Bool | Recovery | Nil = nil,
                   recovery_max_attempts : Int32? = nil,
                   recovery_initial_delay : Time::Span? = nil,
                   recovery_max_delay : Time::Span? = nil,
                   tls_context : OpenSSL::SSL::Context::Client? = nil) : Config
      uri = begin
        URI.parse(uri_str)
      rescue ex : URI::Error
        raise UriError.new("invalid URI: #{ex.message}")
      end

      scheme = uri.scheme || raise UriError.new("missing scheme: #{uri_str}")
      unless scheme == "amqp" || scheme == "amqps"
        raise UriError.new("unsupported scheme: #{scheme}")
      end

      host = uri.host
      raise UriError.new("missing host: #{uri_str}") if host.nil? || host.empty?
      port = uri.port || (scheme == "amqps" ? 5671 : 5672)

      eff_user = user || uri.user || "guest"
      eff_pass = password || uri.password || "guest"
      eff_vhost = vhost || parse_vhost(uri.path)
      query = uri.query_params
      validate_query_keys(query)
      eff_heartbeat = heartbeat || query_span_seconds(query["heartbeat"]?)
      eff_channel_max = channel_max || query_u16(query["channel_max"]?)
      eff_frame_max = frame_max || query_u32(query["frame_max"]?)
      eff_max_body_size = max_body_size || query_u64_positive(query["max_body_size"]?)
      eff_connect_timeout = connect_timeout || query_span_seconds(query["connect_timeout"]?)
      eff_tcp_nodelay = tcp_nodelay.nil? ? query_bool(query["tcp_nodelay"]?) : tcp_nodelay
      eff_buffer_size = buffer_size || query_i32_nonnegative(query["buffer_size"]?)
      eff_recovery = recovery.nil? ? query_recovery(query["recovery"]?) : parse_recovery(recovery)
      eff_product = product || query["product"]?
      eff_information = information || query["information"]?

      new(
        scheme: scheme,
        host: host,
        port: port,
        user: eff_user,
        password: eff_pass,
        vhost: eff_vhost,
        heartbeat: eff_heartbeat || 60.seconds,
        channel_max: eff_channel_max || 2047_u16,
        frame_max: eff_frame_max || 131_072_u32,
        max_body_size: eff_max_body_size || 64_u64 * 1024_u64 * 1024_u64,
        connect_timeout: eff_connect_timeout || 30.seconds,
        tcp_nodelay: eff_tcp_nodelay || false,
        buffer_size: eff_buffer_size || 16_384,
        product: eff_product || "amqp-ng",
        information: eff_information,
        recovery: eff_recovery,
        recovery_max_attempts: recovery_max_attempts || 5,
        recovery_initial_delay: recovery_initial_delay || 500.milliseconds,
        recovery_max_delay: recovery_max_delay || 10.seconds,
        tls_context: tls_context,
      )
    end

    private def self.parse_vhost(path : String?) : String
      return "/" if path.nil? || path.empty? || path == "/"
      stripped = path.starts_with?('/') ? path[1..] : path
      decoded = URI.decode(stripped)
      decoded.empty? ? "/" : decoded
    end

    private def self.validate_query_keys(query : URI::Params) : Nil
      query.each do |key, _value|
        unless RECOGNIZED_QUERY_KEYS.includes?(key)
          raise UriError.new("unknown URI query key '#{key}' (recognized: #{RECOGNIZED_QUERY_KEYS.join(", ")})")
        end
      end
    end

    private def self.query_span_seconds(value : String?) : Time::Span?
      value.try { |v| parse_u32_query(v, "seconds").seconds }
    end

    private def self.query_u16(value : String?) : UInt16?
      value.try { |v| parse_u32_query(v, "UInt16").to_u16 }
    rescue OverflowError
      raise UriError.new("invalid UInt16 query value")
    end

    private def self.query_u32(value : String?) : UInt32?
      value.try { |v| parse_u32_query(v, "UInt32") }
    rescue OverflowError
      raise UriError.new("invalid UInt32 query value")
    end

    private def self.query_u64_positive(value : String?) : UInt64?
      value.try do |v|
        parsed = parse_u64_query(v, "UInt64")
        raise UriError.new("invalid positive UInt64 query value '#{v}'") if parsed == 0
        parsed
      end
    end

    private def self.query_i32_nonnegative(value : String?) : Int32?
      value.try do |v|
        parsed = parse_u32_query(v, "Int32")
        raise UriError.new("invalid Int32 query value '#{v}'") if parsed > Int32::MAX
        parsed.to_i32
      end
    end

    private def self.query_bool(value : String?) : Bool
      case value
      when Nil, "false"
        false
      when "true"
        true
      else
        raise UriError.new("invalid Bool query value '#{value}'")
      end
    end

    private def self.parse_u32_query(value : String, label : String) : UInt32
      unless value.matches?(/\A\d+\z/)
        raise UriError.new("invalid #{label} query value '#{value}'")
      end
      value.to_u32
    rescue ArgumentError | OverflowError
      raise UriError.new("invalid #{label} query value '#{value}'")
    end

    private def self.parse_u64_query(value : String, label : String) : UInt64
      unless value.matches?(/\A\d+\z/)
        raise UriError.new("invalid #{label} query value '#{value}'")
      end
      value.to_u64
    rescue ArgumentError | OverflowError
      raise UriError.new("invalid #{label} query value '#{value}'")
    end

    private def self.query_recovery(value : String?) : Bool
      case value
      when Nil, "none"
        false
      when "full"
        true
      else
        raise UriError.new("invalid recovery query value '#{value}'")
      end
    end

    private def self.parse_recovery(value : Bool | Recovery | Nil) : Bool
      case value
      when Nil
        false
      when Bool
        value
      when Recovery
        value.full?
      else
        false
      end
    end
  end
end
