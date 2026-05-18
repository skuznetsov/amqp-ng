require "./properties"

module Amqp
  # Prepared fixed-route publisher for repeated fire-and-forget publishes.
  #
  # It precomputes the AMQP basic.publish method frame and lets Channel bypass
  # repeated route/method-frame selection on the hot path. Confirm-mode
  # channels deliberately fall back to the normal confirm paths so recovery
  # replay semantics stay identical.
  class PreparedPublisher
    getter channel : Channel
    getter exchange : String
    getter routing_key : String
    getter properties : Properties
    getter mandatory : Bool
    getter immediate : Bool
    getter method_frame : Bytes

    protected def initialize(@channel : Channel,
                             @exchange : String,
                             @routing_key : String,
                             @properties : Properties,
                             @mandatory : Bool,
                             @immediate : Bool,
                             @method_frame : Bytes)
    end

    def publish(body : Bytes) : UInt64?
      @channel.__publish_prepared(self, body)
    end

    def publish(body : String) : UInt64?
      publish(body.to_slice)
    end

    def publish_batch(bodies : Array(Bytes)) : Array(UInt64?)
      @channel.__publish_prepared_batch(self, bodies)
    end
  end
end
