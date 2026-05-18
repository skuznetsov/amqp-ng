require "./arguments"
require "./properties"

module Amqp
  # High-level exchange wrapper for amqp-client.cr-style migration code.
  # New code can use Channel methods directly.
  class Exchange
    getter channel : Channel
    getter name : String

    def initialize(@channel : Channel, @name : String)
    end

    def bind(exchange : String, routing_key : String = "", no_wait : Bool = false, args arguments : Arguments | NamedTuple = Arguments.new) : self
      arguments = Amqp.coerce_arguments(arguments)
      @channel.exchange_bind(@name, exchange, routing_key, arguments)
      self
    end

    def unbind(exchange : String, routing_key : String = "", no_wait : Bool = false, args arguments : Arguments | NamedTuple = Arguments.new) : self
      arguments = Amqp.coerce_arguments(arguments)
      @channel.exchange_unbind(@name, exchange, routing_key, arguments)
      self
    end

    def publish(body : Bytes,
                routing_key : String = "",
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new) : UInt64
      @channel.basic_publish(body, @name, routing_key, mandatory, immediate, properties)
    end

    def publish(body : String,
                routing_key : String = "",
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new) : UInt64
      publish(body.to_slice, routing_key, mandatory, immediate, properties)
    end

    def publish(body : Bytes,
                routing_key : String = "",
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new,
                &callback : Bool -> Nil) : UInt64
      @channel.basic_publish(body, @name, routing_key, mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def publish(body : String,
                routing_key : String = "",
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new,
                &callback : Bool -> Nil) : UInt64
      publish(body.to_slice, routing_key, mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def publish_confirm(body : Bytes,
                        routing_key : String = "",
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties : Properties = Properties.new,
                        timeout : Time::Span = 30.seconds) : Bool
      @channel.basic_publish_confirm(body, @name, routing_key, mandatory, immediate, properties, timeout: timeout)
    end

    def publish_confirm(body : String,
                        routing_key : String = "",
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties : Properties = Properties.new,
                        timeout : Time::Span = 30.seconds) : Bool
      publish_confirm(body.to_slice, routing_key, mandatory, immediate, properties, timeout: timeout)
    end

    def delete(if_unused : Bool = false) : Nil
      @channel.exchange_delete(@name, if_unused: if_unused)
    end
  end
end
