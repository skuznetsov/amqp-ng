require "./amqp/error"
require "./amqp/arguments"
require "./amqp/config"
require "./amqp/message"
require "./amqp/get_message"
require "./amqp/connection"
require "./amqp/queue"
require "./amqp/exchange"

module Amqp
  VERSION = "0.1.0"

  def self.connect(uri : String, **kw) : Connection
    config = Config.parse(uri, **kw)
    Connection.connect(config)
  end

  def self.connect(uri : URI, **kw) : Connection
    connect(uri.to_s, **kw)
  end

  def self.connect(uri : String, **kw, & : Connection -> _)
    conn = connect(uri, **kw)
    begin
      yield conn
    ensure
      conn.close
    end
  end

  def self.connect(uri : URI, **kw, & : Connection -> _)
    connect(uri.to_s, **kw) { |conn| yield conn }
  end

  def self.tls_context_default : OpenSSL::SSL::Context::Client
    OpenSSL::SSL::Context::Client.new
  end
end
