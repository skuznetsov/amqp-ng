require "./properties"
require "./wire/amqp_zero_nine_one/content_header"

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
    @encoded_properties : Amqp::Wire::AmqpZeroNineOne::ContentHeader::EncodedProperties?
    @header_frame_candidate_body_size : UInt64?
    @cached_header_frame : Bytes?
    @cached_header_frame_body_size : UInt64?

    protected def initialize(@channel : Channel,
                             @exchange : String,
                             @routing_key : String,
                             @properties : Properties,
                             @mandatory : Bool,
                             @immediate : Bool,
                             @method_frame : Bytes)
      @encoded_properties = unless @properties.empty?
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode_properties(@properties)
      end
      @header_frame_candidate_body_size = nil
      @cached_header_frame = nil
      @cached_header_frame_body_size = nil
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

    # :nodoc:
    def __encoded_properties : Amqp::Wire::AmqpZeroNineOne::ContentHeader::EncodedProperties?
      @encoded_properties
    end

    # :nodoc:
    def __content_header_frame(body_size : UInt64) : Bytes?
      encoded = @encoded_properties
      return nil unless encoded

      if frame = @cached_header_frame
        return frame if @cached_header_frame_body_size == body_size
      end

      if @header_frame_candidate_body_size == body_size
        frame = Amqp::Wire::AmqpZeroNineOne::ContentHeader.frame(
          @channel.id,
          Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          body_size,
          encoded,
        )
        @cached_header_frame = frame
        @cached_header_frame_body_size = body_size
        return frame
      end

      @header_frame_candidate_body_size = body_size
      nil
    end
  end
end
