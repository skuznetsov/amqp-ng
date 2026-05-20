require "spec"
require "../src/amqp"

module SpecHelper
  extend self

  AMQP_URL_ENV = "AMQP_URL"
  DEFAULT_URL  = "amqp://guest:guest@127.0.0.1:5672/"

  def amqp_url : String
    ENV[AMQP_URL_ENV]? || DEFAULT_URL
  end

  # Returns true only after a minimal AMQP session can be opened.
  # RabbitMQ can accept TCP before the AMQP application is ready; treating
  # that as reachable makes live specs fail with handshake EOF races.
  def broker_reachable? : Bool
    conn = Amqp.connect(amqp_url, heartbeat: 0.seconds, connect_timeout: 0.5.seconds)
    conn.close
    true
  rescue
    false
  end
end
