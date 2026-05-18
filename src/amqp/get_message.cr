require "./properties"
require "./queue_info"
require "./delivery"

module Amqp
  struct GetMessage
    getter body : Bytes
    getter properties : Properties
    getter delivery_tag : UInt64
    getter redelivered : Bool
    getter exchange : String
    getter routing_key : String
    getter message_count : UInt32
    @channel : Channel?

    def initialize(@body, @properties, @delivery_tag, @redelivered,
                   @exchange, @routing_key, @message_count, @channel = nil)
    end

    def body_io : IO::Memory
      IO::Memory.new(@body)
    end

    def ack(multiple : Bool = false) : Nil
      (@channel || raise ConfigurationError.new("get message has no channel")).ack(@delivery_tag, multiple: multiple)
    end

    def nack(multiple : Bool = false, requeue : Bool = true) : Nil
      (@channel || raise ConfigurationError.new("get message has no channel")).nack(@delivery_tag, multiple: multiple, requeue: requeue)
    end

    def reject(requeue : Bool = true) : Nil
      (@channel || raise ConfigurationError.new("get message has no channel")).reject(@delivery_tag, requeue: requeue)
    end
  end

  alias QueueDeclareOk = QueueInfo
  alias DeliverMessage = Delivery
end
