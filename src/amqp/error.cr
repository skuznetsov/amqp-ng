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

  class ChannelRpcTimeoutError < ChannelError
  end

  class PublishNackError < ChannelError
    getter delivery_tag : UInt64

    def initialize(@delivery_tag)
      super("publish #{@delivery_tag} was nacked by broker")
    end
  end

  class PublishTimeoutError < ChannelError
    getter delivery_tag : UInt64
    getter timeout : Time::Span

    def initialize(@delivery_tag, @timeout)
      super("publish #{@delivery_tag} was not confirmed within #{@timeout}")
    end
  end

  class PublishReturnedError < ChannelError
    getter delivery_tag : UInt64
    getter reason : ReturnReason

    def initialize(@delivery_tag, @reason)
      super("publish #{@delivery_tag} was returned by broker: #{@reason.reply_code} #{@reason.reply_text}")
    end
  end

  class PublishOutOfOrderError < ChannelError
    getter delivery_tag : UInt64

    def initialize(@delivery_tag)
      super("broker confirmed unknown publish tag #{@delivery_tag}")
    end
  end

  class PreconditionFailedError < ChannelError
    getter reply_code : UInt16
    getter reply_text : String

    def initialize(@reply_code, @reply_text)
      super("precondition failed: #{@reply_code} #{@reply_text}")
    end
  end
end
