require "./arguments"
require "./properties"

module Amqp
  # High-level queue wrapper for amqp-client.cr-style migration code.
  # New code can use Channel methods directly.
  class Queue
    getter channel : Channel
    getter name : String

    def initialize(@channel : Channel, @name : String)
    end

    def bind(exchange : String, routing_key : String = "", no_wait : Bool = false, args arguments : Arguments | NamedTuple = Arguments.new) : self
      arguments = Amqp.coerce_arguments(arguments)
      @channel.queue_bind(@name, exchange, routing_key, arguments, no_wait: no_wait)
      self
    end

    def unbind(exchange : String, routing_key : String = "", args arguments : Arguments | NamedTuple = Arguments.new) : self
      arguments = Amqp.coerce_arguments(arguments)
      @channel.queue_unbind(@name, exchange, routing_key, arguments)
      self
    end

    def publish(body : Bytes,
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new) : UInt64
      @channel.basic_publish(body, "", @name, mandatory, immediate, properties)
    end

    def publish(body : String,
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new) : UInt64
      publish(body.to_slice, mandatory, immediate, properties)
    end

    def publish(io : IO,
                bytesize : Int,
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new) : UInt64
      @channel.basic_publish(io, bytesize, "", @name, mandatory, immediate, properties)
    end

    def publish(body : Bytes,
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new,
                &callback : Bool -> Nil) : UInt64
      @channel.basic_publish(body, "", @name, mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def publish(body : String,
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new,
                &callback : Bool -> Nil) : UInt64
      publish(body.to_slice, mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def publish(io : IO,
                bytesize : Int,
                mandatory : Bool = false,
                immediate : Bool = false,
                props properties : Properties = Properties.new,
                &callback : Bool -> Nil) : UInt64
      @channel.basic_publish(io, bytesize, "", @name, mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def publish_confirm(body : Bytes,
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties : Properties = Properties.new,
                        timeout : Time::Span = 30.seconds) : Bool
      @channel.basic_publish_confirm(body, "", @name, mandatory, immediate, properties, timeout: timeout)
    end

    def publish_confirm(body : String,
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties : Properties = Properties.new,
                        timeout : Time::Span = 30.seconds) : Bool
      publish_confirm(body.to_slice, mandatory, immediate, properties, timeout: timeout)
    end

    def publish_confirm(io : IO,
                        bytesize : Int,
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties : Properties = Properties.new,
                        timeout : Time::Span = 30.seconds) : Bool
      @channel.basic_publish_confirm(io, bytesize, "", @name, mandatory, immediate,
        properties, timeout: timeout)
    end

    def get(no_ack : Bool = true) : GetMessage?
      @channel.basic_get(@name, no_ack)
    end

    def subscribe(tag : String = "",
                  no_ack : Bool = true,
                  exclusive : Bool = false,
                  block : Bool = false,
                  args arguments : Arguments | NamedTuple = Arguments.new,
                  work_pool : Int32 = 1,
                  &callback : DeliverMessage -> Nil) : String
      arguments = Amqp.coerce_arguments(arguments)
      @channel.basic_consume(@name, tag, no_ack, exclusive, block, arguments, work_pool) do |delivery|
        callback.call(delivery)
      end
    end

    def unsubscribe(consumer_tag : String, no_wait : Bool = true) : self
      @channel.basic_cancel(consumer_tag, no_wait)
      self
    end

    def purge : UInt32
      @channel.queue_purge(@name)
    end

    def delete(if_unused : Bool = false, if_empty : Bool = false) : UInt32
      @channel.queue_delete(@name, if_unused, if_empty)
    end

    def message_count : UInt32
      @channel.queue_declare(@name, passive: true).message_count
    end

    def consumer_count : UInt32
      @channel.queue_declare(@name, passive: true).consumer_count
    end
  end
end
