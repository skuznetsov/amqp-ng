module Amqp
  abstract class Error < ::Exception
  end

  class ConfigurationError < Error
  end

  class UriError < ConfigurationError
  end

  class TlsConfigError < ConfigurationError
  end

  abstract class ConnectError < Error
  end

  class ConnectTimeoutError < ConnectError
  end

  class ConnectRefusedError < ConnectError
  end

  class AuthenticationError < ConnectError
  end

  class VhostAccessError < ConnectError
  end

  class TlsHandshakeError < ConnectError
  end

  class ProtocolNegotiationError < ConnectError
  end

  abstract class ConnectionError < Error
  end

  class ConnectionClosedByBroker < ConnectionError
    getter reply_code : UInt16
    getter reply_text : String
    getter origin_class_id : UInt16
    getter origin_method_id : UInt16

    def initialize(@reply_code, @reply_text, @origin_class_id, @origin_method_id)
      super("connection closed by broker: #{@reply_code} #{@reply_text}")
    end
  end

  class ConnectionClosedByCaller < ConnectionError
  end

  class SocketError < ConnectionError
  end

  class ProtocolError < ConnectionError
  end

  class FrameTooLargeError < ConnectionError
  end

  class HeartbeatTimeoutError < ConnectionError
  end

  # Raised on operations attempted while the connection is recovering.
  class RecoveryInProgress < ConnectionError
  end

  # Raised when automatic recovery has exhausted its retry budget.
  class RecoveryExhaustedError < ConnectionError
  end

  abstract class ChannelError < Error
  end

  class ChannelClosedByBroker < ChannelError
    getter reply_code : UInt16
    getter reply_text : String
    getter origin_class_id : UInt16
    getter origin_method_id : UInt16

    def initialize(@reply_code, @reply_text, @origin_class_id, @origin_method_id)
      super("channel closed by broker: #{@reply_code} #{@reply_text}")
    end
  end

  class ChannelClosedByCaller < ChannelError
  end

  class ChannelLimitError < ChannelError
  end

  class ConcurrencyError < ChannelError
  end

  class PreconditionFailedError < ChannelError
    getter reply_code : UInt16
    getter reply_text : String

    def initialize(@reply_code, @reply_text)
      super("precondition failed: #{@reply_code} #{@reply_text}")
    end
  end
end
