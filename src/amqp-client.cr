require "json"
require "./amqp"

{% unless @top_level.has_constant?(:AMQ) %}
  module AMQ
    module Protocol
      alias Field = ::Amqp::FieldValue
      alias Table = Hash(String, ::Amqp::FieldValue)

      struct Properties
        property content_type : String?
        property content_encoding : String?
        property headers : Table?
        property delivery_mode : UInt8?
        property priority : UInt8?
        property correlation_id : String?
        property reply_to : String?
        property expiration : String?
        property message_id : String?
        property timestamp_raw : Int64?
        property type : String?
        property user_id : String?
        property app_id : String?

        def initialize
        end

        def self.from_json(data : JSON::Any) : self
          props = new
          props.content_type = data["content_type"]?.try(&.as_s)
          props.content_encoding = data["content_encoding"]?.try(&.as_s)
          props.headers = data["headers"]?.try(&.as_h?).try do |hash|
            ::AMQP::Client.coerce_arguments(hash)
          end
          props.delivery_mode = data["delivery_mode"]?.try(&.as_i?.try(&.to_u8))
          props.priority = data["priority"]?.try(&.as_i?.try(&.to_u8))
          props.correlation_id = data["correlation_id"]?.try(&.as_s)
          props.reply_to = data["reply_to"]?.try(&.as_s)
          exp = data["expiration"]?
          props.expiration = exp.try { |value| value.as_s? || value.as_i64?.try(&.to_s) }
          props.message_id = data["message_id"]?.try(&.as_s)
          props.timestamp_raw = data["timestamp"]?.try(&.as_i64?)
          props.type = data["type"]?.try(&.as_s)
          props.user_id = data["user_id"]?.try(&.as_s)
          props.app_id = data["app_id"]?.try(&.as_s)
          props
        end
      end
    end
  end
{% end %}

module AMQP
  # Compatibility facade for migration paths that currently require
  # `amqp-client`. The implementation delegates to amqp-ng and keeps the
  # narrower old-client shapes used by LavinMQ's shovel/perf code.
  class Client
    alias Error = ::Amqp::Error
    alias DeliverMessage = ::Amqp::Delivery
    alias GetMessage = ::Amqp::GetMessage
    alias Properties = ::Amqp::Properties

    class Arguments < Hash(String, ::Amqp::FieldValue)
      def initialize
        super()
      end

      def initialize(hash : Hash)
        super()
        hash.each do |key, value|
          self[key.to_s] = ::AMQP::Client.coerce_field(value)
        end
      end
    end

    class Connection
      alias ClosedException = ::Amqp::ConnectionError

      getter inner : ::Amqp::Connection

      def initialize(@inner : ::Amqp::Connection)
      end

      def channel : Channel
        Channel.new(@inner.channel)
      end

      def channel(id : UInt16) : Channel
        Channel.new(@inner.channel(id))
      end

      def close(no_wait : Bool = false) : Nil
        @inner.close
      end

      def closed? : Bool
        @inner.closed?
      end

      def write(frame : Frame::Basic::Consume) : Nil
        @inner.__compat_write_method_frame(frame.channel, frame.to_payload)
      end
    end

    module Frame
      module Basic
        struct Consume
          getter channel : UInt16

          def initialize(@channel : UInt16,
                         @reserved1 : UInt16,
                         @queue : String,
                         @consumer_tag : String,
                         @no_local : Bool,
                         @no_ack : Bool,
                         @exclusive : Bool,
                         @no_wait : Bool,
                         @arguments : Arguments)
          end

          def to_payload : Bytes
            io = IO::Memory.new
            io.write_bytes(::Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
              IO::ByteFormat::NetworkEndian)
            io.write_bytes(::Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CONSUME,
              IO::ByteFormat::NetworkEndian)
            io.write_bytes(@reserved1, IO::ByteFormat::NetworkEndian)
            ::Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(io, @queue)
            ::Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(io, @consumer_tag)
            ::Amqp::Wire::AmqpZeroNineOne::BitPack.write(io,
              [@no_local, @no_ack, @exclusive, @no_wait])
            ::Amqp::Wire::AmqpZeroNineOne::Types.write_field_table(io, @arguments)
            io.to_slice
          end
        end
      end
    end

    class Channel
      alias ClosedException = ::Amqp::ChannelError

      getter inner : ::Amqp::Channel

      def initialize(@inner : ::Amqp::Channel)
      end

      def id : UInt16
        @inner.id
      end

      def closed? : Bool
        @inner.closed?
      end

      def close : Nil
        @inner.close
      end

      def confirm_select : Nil
        @inner.confirm_select
      end

      def wait_for_confirms(timeout : Time::Span = 60.seconds) : Bool
        @inner.wait_for_confirms(timeout)
      end

      def tx_select : Nil
        @inner.tx_select
      end

      def tx_commit : Nil
        @inner.tx_commit
      end

      def tx_rollback : Nil
        @inner.tx_rollback
      end

      def prefetch(count : UInt16, *, global : Bool = false) : Nil
        @inner.prefetch(count, global: global)
      end

      def queue_declare(name : String = "",
                        passive : Bool = false,
                        durable : Bool = false,
                        exclusive : Bool = false,
                        auto_delete : Bool = false,
                        no_wait : Bool = false,
                        args arguments = Arguments.new)
        if no_wait
          info = @inner.__compat_queue_declare_no_wait(name, passive, durable,
            exclusive, auto_delete, ::AMQP::Client.coerce_arguments(arguments))
          return {queue_name: info.name, message_count: info.message_count, consumer_count: info.consumer_count}
        end

        info = @inner.queue_declare(name, passive, durable, exclusive, auto_delete,
          ::AMQP::Client.coerce_arguments(arguments))
        {queue_name: info.name, message_count: info.message_count, consumer_count: info.consumer_count}
      end

      def queue(name : String = "",
                passive : Bool = false,
                durable : Bool = true,
                exclusive : Bool = false,
                auto_delete : Bool = false,
                args arguments = Arguments.new) : Queue
        info = @inner.queue_declare(name, passive, durable, exclusive, auto_delete,
          ::AMQP::Client.coerce_arguments(arguments))
        Queue.new(self, info.name)
      end

      def queue_bind(queue : String,
                     exchange : String,
                     routing_key : String = "",
                     args arguments = Arguments.new,
                     no_wait : Bool = false) : Nil
        @inner.queue_bind(queue, exchange, routing_key,
          ::AMQP::Client.coerce_arguments(arguments), no_wait: no_wait)
      end

      def queue_delete(name : String,
                       if_unused : Bool = false,
                       if_empty : Bool = false) : UInt32
        @inner.queue_delete(name, if_unused, if_empty)
      end

      def exchange_delete(name : String,
                          if_unused : Bool = false) : Nil
        @inner.exchange_delete(name, if_unused)
      end

      def exchange(name : String,
                   type : String,
                   passive : Bool = false,
                   durable : Bool = true,
                   internal : Bool = false,
                   auto_delete : Bool = false,
                   args arguments = Arguments.new) : Exchange
        @inner.exchange_declare(name, type, passive, durable, auto_delete, internal,
          ::AMQP::Client.coerce_arguments(arguments))
        Exchange.new(self, name)
      end

      def basic_consume(queue : String,
                        tag : String = "",
                        no_ack : Bool = true,
                        exclusive : Bool = false,
                        block : Bool = false,
                        args arguments = Arguments.new,
                        work_pool : Int32 = 1,
                        &callback : DeliverMessage -> Nil) : String
        @inner.basic_consume(queue, tag, no_ack, exclusive, block,
          ::AMQP::Client.coerce_arguments(arguments), work_pool) do |delivery|
          callback.call(delivery)
        end
      end

      def basic_cancel(consumer_tag : String, no_wait : Bool = false) : Nil
        @inner.basic_cancel(consumer_tag, no_wait)
      end

      def basic_ack(delivery_tag : UInt64, multiple : Bool = false) : Nil
        @inner.basic_ack(delivery_tag, multiple)
      end

      def basic_nack(delivery_tag : UInt64, multiple : Bool = false, requeue : Bool = true) : Nil
        @inner.basic_nack(delivery_tag, multiple, requeue)
      end

      def basic_reject(delivery_tag : UInt64, requeue : Bool = true) : Nil
        @inner.basic_reject(delivery_tag, requeue)
      end

      def basic_get(queue : String, no_ack : Bool = true) : GetMessage?
        @inner.basic_get(queue, no_ack)
      end

      def basic_publish(body : Bytes,
                        exchange : String,
                        routing_key : String = "",
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties = Properties.new) : UInt64
        @inner.basic_publish(body, exchange, routing_key, mandatory, immediate,
          ::AMQP::Client.coerce_properties(properties))
      end

      def basic_publish(body : String,
                        exchange : String,
                        routing_key : String = "",
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties = Properties.new) : UInt64
        basic_publish(body.to_slice, exchange, routing_key, mandatory, immediate, properties)
      end

      def basic_publish(io : IO,
                        exchange : String,
                        routing_key : String = "",
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties = Properties.new) : UInt64
        @inner.basic_publish(io, exchange, routing_key, mandatory, immediate,
          ::AMQP::Client.coerce_properties(properties))
      end

      def basic_publish(body : Bytes,
                        exchange : String,
                        routing_key : String = "",
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties = Properties.new,
                        &callback : Bool -> Nil) : UInt64
        @inner.basic_publish(body, exchange, routing_key, mandatory, immediate,
          ::AMQP::Client.coerce_properties(properties)) do |ok|
          callback.call(ok)
        end
      end

      def basic_publish(io : IO,
                        exchange : String,
                        routing_key : String = "",
                        mandatory : Bool = false,
                        immediate : Bool = false,
                        props properties = Properties.new,
                        &callback : Bool -> Nil) : UInt64
        @inner.basic_publish(io, exchange, routing_key, mandatory, immediate,
          ::AMQP::Client.coerce_properties(properties)) do |ok|
          callback.call(ok)
        end
      end
    end

    class Queue
      getter channel : Channel
      getter name : String

      def initialize(@channel : Channel, @name : String)
      end

      def bind(exchange : String, routing_key : String = "", no_wait : Bool = false, args arguments = Arguments.new) : self
        @channel.queue_bind(@name, exchange, routing_key, no_wait: no_wait, args: arguments)
        self
      end

      def purge : UInt32
        @channel.inner.queue_purge(@name)
      end

      def delete(if_unused : Bool = false, if_empty : Bool = false) : UInt32
        @channel.inner.queue_delete(@name, if_unused, if_empty)
      end

      def get(no_ack : Bool = true) : GetMessage?
        @channel.basic_get(@name, no_ack)
      end

      def subscribe(tag : String = "",
                    no_ack : Bool = true,
                    exclusive : Bool = false,
                    block : Bool = false,
                    args arguments = Arguments.new,
                    work_pool : Int32 = 1,
                    &callback : DeliverMessage -> Nil) : String
        @channel.basic_consume(@name, tag, no_ack, exclusive, block, arguments, work_pool) do |delivery|
          callback.call(delivery)
        end
      end

      def unsubscribe(consumer_tag : String, no_wait : Bool = true) : self
        @channel.basic_cancel(consumer_tag, no_wait)
        self
      end
    end

    class Exchange
      getter channel : Channel
      getter name : String

      def initialize(@channel : Channel, @name : String)
      end

      def bind(source : String, routing_key : String = "", no_wait : Bool = false, args arguments = Arguments.new) : self
        @channel.inner.exchange_bind(@name, source, routing_key,
          ::AMQP::Client.coerce_arguments(arguments), no_wait: no_wait)
        self
      end

      def unbind(source : String, routing_key : String = "", args arguments = Arguments.new, no_wait : Bool = false) : self
        @channel.inner.exchange_unbind(@name, source, routing_key,
          ::AMQP::Client.coerce_arguments(arguments), no_wait: no_wait)
        self
      end

      def publish(body : Bytes,
                  routing_key : String = "",
                  mandatory : Bool = false,
                  immediate : Bool = false,
                  props properties = Properties.new) : UInt64
        @channel.basic_publish(body, @name, routing_key, mandatory, immediate, properties)
      end

      def publish(io : IO,
                  routing_key : String = "",
                  mandatory : Bool = false,
                  immediate : Bool = false,
                  props properties = Properties.new) : UInt64
        @channel.basic_publish(io, @name, routing_key, mandatory, immediate, properties)
      end

      def publish(body : Bytes,
                  routing_key : String = "",
                  mandatory : Bool = false,
                  immediate : Bool = false,
                  props properties = Properties.new,
                  &callback : Bool -> Nil) : UInt64
        @channel.basic_publish(body, @name, routing_key, mandatory, immediate, properties) do |ok|
          callback.call(ok)
        end
      end

      def publish(io : IO,
                  routing_key : String = "",
                  mandatory : Bool = false,
                  immediate : Bool = false,
                  props properties = Properties.new,
                  &callback : Bool -> Nil) : UInt64
        @channel.basic_publish(io, @name, routing_key, mandatory, immediate, properties) do |ok|
          callback.call(ok)
        end
      end
    end

    def initialize(@uri : URI | String)
    end

    def host=(host : String) : String
      uri = @uri.is_a?(URI) ? @uri.as(URI) : URI.parse(@uri.as(String))
      uri.host = host
      @uri = uri
      host
    end

    def connect : Connection
      Connection.new(::Amqp.connect(@uri.to_s))
    end

    def self.start(uri : URI | String, & : Connection -> _)
      conn = new(uri).connect
      begin
        yield conn
      ensure
        conn.close
      end
    end

    def self.coerce_arguments(arguments : ::Amqp::Arguments) : ::Amqp::Arguments
      arguments
    end

    def self.coerce_arguments(arguments : Arguments) : ::Amqp::Arguments
      arguments
    end

    def self.coerce_arguments(arguments) : ::Amqp::Arguments
      converted = ::Amqp::Arguments.new
      arguments.each do |key, value|
        converted[key.to_s] = coerce_field(value)
      end
      converted
    end

    def self.coerce_properties(properties : ::Amqp::Properties) : ::Amqp::Properties
      properties
    end

    def self.coerce_properties(properties) : ::Amqp::Properties
      delivery_mode = case mode = properties.delivery_mode
                      when ::Amqp::Properties::Persistence
                        mode
                      when UInt8
                        mode == 2_u8 ? ::Amqp::Properties::Persistence::Persistent : ::Amqp::Properties::Persistence::Transient
                      else
                        nil
                      end
      timestamp = if properties.responds_to?(:timestamp_raw)
                    raw = properties.timestamp_raw
                    raw ? Time.unix(raw) : nil
                  elsif properties.responds_to?(:timestamp)
                    properties.timestamp
                  end
      ::Amqp::Properties.new(
        content_type: properties.content_type,
        content_encoding: properties.content_encoding,
        headers: properties.headers.try { |headers| coerce_arguments(headers) },
        delivery_mode: delivery_mode,
        priority: properties.priority,
        correlation_id: properties.correlation_id,
        reply_to: properties.reply_to,
        expiration: properties.expiration,
        message_id: properties.message_id,
        timestamp: timestamp,
        type: properties.type,
        user_id: properties.user_id,
        app_id: properties.app_id,
      )
    end

    def self.coerce_field(value : JSON::Any) : ::Amqp::FieldValue
      if value.raw.nil?
        nil
      elsif bool = value.as_bool?
        bool
      elsif int = value.as_i64?
        int
      elsif float = value.as_f?
        float
      elsif string = value.as_s?
        string
      elsif array = value.as_a?
        array.map { |item| coerce_field(item) }
      elsif hash = value.as_h?
        coerce_arguments(hash)
      else
        value.to_s
      end
    end

    def self.coerce_field(value : Hash) : ::Amqp::FieldValue
      coerce_arguments(value)
    end

    def self.coerce_field(value : Array) : ::Amqp::FieldValue
      converted = [] of ::Amqp::FieldValue
      value.each { |item| converted << coerce_field(item) }
      converted
    end

    def self.coerce_field(value) : ::Amqp::FieldValue
      case value
      when Nil, Bool, Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, Float32, Float64, String, Time, Bytes
        value
      when Int
        value.to_i64
      when Float
        value.to_f64
      when Hash
        coerce_arguments(value)
      when Array
        converted = [] of ::Amqp::FieldValue
        value.each { |item| converted << coerce_field(item) }
        converted
      else
        if value.responds_to?(:each)
          coerce_arguments(value)
        else
          value.to_s
        end
      end
    end
  end
end
