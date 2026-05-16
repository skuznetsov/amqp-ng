require "./properties"

module Amqp
  struct Message
    getter body : Bytes
    getter properties : Properties

    def initialize(@body : Bytes, @properties : Properties = Properties.new)
    end

    def self.new(body : String, properties : Properties = Properties.new) : Message
      new(body.to_slice, properties)
    end

    def self.new(io : IO, properties : Properties = Properties.new) : Message
      new(io.gets_to_end.to_slice, properties)
    end
  end

  struct ReturnReason
    getter reply_code : UInt16
    getter reply_text : String
    getter exchange : String
    getter routing_key : String

    def initialize(@reply_code, @reply_text, @exchange, @routing_key)
    end
  end

  struct ReturnedMessage
    getter reply_code : UInt16
    getter reply_text : String
    getter exchange : String
    getter routing_key : String
    getter properties : Properties
    getter body : Bytes

    def initialize(@reply_code, @reply_text, @exchange, @routing_key, @properties, @body)
    end

    def reason : ReturnReason
      ReturnReason.new(@reply_code, @reply_text, @exchange, @routing_key)
    end
  end

  struct ConfirmOutcome
    enum Kind
      Ack
      Nack
      Returned
    end

    getter kind : Kind
    getter delivery_tag : UInt64
    getter return_reason : ReturnReason?

    def initialize(@kind : Kind, @delivery_tag : UInt64, @return_reason : ReturnReason? = nil)
    end
  end

  enum Recovery
    None
    Full
  end

  alias Persistence = Properties::Persistence
end
