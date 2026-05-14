require "spec"
require "../src/amqp"

module SpecHelper
  extend self

  AMQP_URL_ENV = "AMQP_URL"
  DEFAULT_URL  = "amqp://guest:guest@127.0.0.1:5672/"

  def amqp_url : String
    ENV[AMQP_URL_ENV]? || DEFAULT_URL
  end

  # Returns true if a TCP connection to the broker is accepted.
  # Specs that require a live broker should skip when this is false.
  def broker_reachable? : Bool
    uri = URI.parse(amqp_url)
    host = uri.host || "127.0.0.1"
    port = uri.port || 5672
    sock = TCPSocket.new(host, port, connect_timeout: 0.5.seconds)
    sock.close
    true
  rescue
    false
  end
end
