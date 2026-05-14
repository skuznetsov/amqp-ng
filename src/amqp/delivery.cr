require "./properties"

module Amqp
  # A message handed to a consumer (basic.deliver/get-ok/return).
  struct Delivery
    getter consumer_tag : String
    getter delivery_tag : UInt64
    getter redelivered : Bool
    getter exchange : String
    getter routing_key : String
    getter properties : Properties
    getter body : Bytes
    @channel : Channel?

    def initialize(@consumer_tag, @delivery_tag, @redelivered, @exchange,
                   @routing_key, @properties, @body, @channel = nil)
    end

    def ack(multiple : Bool = false) : Nil
      (@channel || raise ConfigurationError.new("delivery has no channel")).ack(@delivery_tag, multiple: multiple)
    end

    def nack(multiple : Bool = false, requeue : Bool = true) : Nil
      (@channel || raise ConfigurationError.new("delivery has no channel")).nack(@delivery_tag, multiple: multiple, requeue: requeue)
    end

    def reject(requeue : Bool = true) : Nil
      (@channel || raise ConfigurationError.new("delivery has no channel")).reject(@delivery_tag, requeue: requeue)
    end
  end
end
