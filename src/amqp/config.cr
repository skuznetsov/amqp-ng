require "uri"
require "./error"

module Amqp
  struct Config
    getter scheme : String
    getter host : String
    getter port : Int32
    getter user : String
    getter password : String
    getter vhost : String
    getter heartbeat : Time::Span
    getter channel_max : UInt16
    getter frame_max : UInt32
    getter connect_timeout : Time::Span
    getter product : String
    getter information : String?
    getter? recovery : Bool
    getter recovery_max_attempts : Int32
    getter recovery_initial_delay : Time::Span
    getter recovery_max_delay : Time::Span

    def initialize(@scheme, @host, @port, @user, @password, @vhost,
                   @heartbeat, @channel_max, @frame_max, @connect_timeout,
                   @product, @information,
                   @recovery = false,
                   @recovery_max_attempts = 5,
                   @recovery_initial_delay = 500.milliseconds,
                   @recovery_max_delay = 10.seconds)
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
                   connect_timeout : Time::Span? = nil,
                   product : String? = nil,
                   information : String? = nil,
                   recovery : Bool? = nil,
                   recovery_max_attempts : Int32? = nil,
                   recovery_initial_delay : Time::Span? = nil,
                   recovery_max_delay : Time::Span? = nil) : Config
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

      new(
        scheme: scheme,
        host: host,
        port: port,
        user: eff_user,
        password: eff_pass,
        vhost: eff_vhost,
        heartbeat: heartbeat || 60.seconds,
        channel_max: channel_max || 2047_u16,
        frame_max: frame_max || 131_072_u32,
        connect_timeout: connect_timeout || 10.seconds,
        product: product || "amqp-ng",
        information: information,
        recovery: recovery.nil? ? false : recovery,
        recovery_max_attempts: recovery_max_attempts || 5,
        recovery_initial_delay: recovery_initial_delay || 500.milliseconds,
        recovery_max_delay: recovery_max_delay || 10.seconds,
      )
    end

    private def self.parse_vhost(path : String?) : String
      return "/" if path.nil? || path.empty? || path == "/"
      stripped = path.starts_with?('/') ? path[1..] : path
      decoded = URI.decode(stripped)
      decoded.empty? ? "/" : decoded
    end
  end
end
