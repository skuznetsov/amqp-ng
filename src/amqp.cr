require "./amqp/error"
require "./amqp/arguments"
require "./amqp/config"
require "./amqp/connection"

module Amqp
  def self.connect(uri : String, **kw) : Connection
    config = Config.parse(uri, **kw)
    Connection.connect(config)
  end

  def self.connect(uri : String, **kw, & : Connection -> _)
    conn = connect(uri, **kw)
    begin
      yield conn
    ensure
      conn.close
    end
  end
end
