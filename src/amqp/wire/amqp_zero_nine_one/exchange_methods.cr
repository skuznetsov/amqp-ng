require "../../error"
require "./types"
require "./bit_pack"
require "./connection_methods"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_EXCHANGE_DECLARE    = 10_u16
  METHOD_ID_EXCHANGE_DECLARE_OK = 11_u16
  METHOD_ID_EXCHANGE_DELETE     = 20_u16
  METHOD_ID_EXCHANGE_DELETE_OK  = 21_u16
  METHOD_ID_EXCHANGE_BIND       = 30_u16
  METHOD_ID_EXCHANGE_BIND_OK    = 31_u16
  METHOD_ID_EXCHANGE_UNBIND     = 40_u16
  METHOD_ID_EXCHANGE_UNBIND_OK  = 51_u16

  module ExchangeMethods
    extend self

    struct Declare
      getter name : String
      getter type : String
      getter passive : Bool
      getter durable : Bool
      getter auto_delete : Bool
      getter internal : Bool
      getter no_wait : Bool
      getter arguments : Amqp::Arguments

      def initialize(@name, @type, @passive, @durable, @auto_delete, @internal,
                     @arguments, @no_wait = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_EXCHANGE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_EXCHANGE_DECLARE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian) # reserved-1
        Types.write_shortstr(io, @name)
        Types.write_shortstr(io, @type)
        BitPack.write(io, [@passive, @durable, @auto_delete, @internal, @no_wait])
        Types.write_field_table(io, @arguments)
        io.to_slice
      end
    end

    struct DeclareOk
      def self.read(io : IO) : self
        new
      end
    end

    struct Delete
      getter name : String
      getter if_unused : Bool
      getter no_wait : Bool

      def initialize(@name, @if_unused, @no_wait = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_EXCHANGE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_EXCHANGE_DELETE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @name)
        BitPack.write(io, [@if_unused, @no_wait])
        io.to_slice
      end
    end

    struct DeleteOk
      def self.read(io : IO) : self
        new
      end
    end

    struct Bind
      getter destination : String
      getter source : String
      getter routing_key : String
      getter no_wait : Bool
      getter arguments : Amqp::Arguments

      def initialize(@destination, @source, @routing_key, @arguments, @no_wait = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_EXCHANGE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_EXCHANGE_BIND, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @destination)
        Types.write_shortstr(io, @source)
        Types.write_shortstr(io, @routing_key)
        BitPack.write(io, [@no_wait])
        Types.write_field_table(io, @arguments)
        io.to_slice
      end
    end

    struct BindOk
      def self.read(io : IO) : self
        new
      end
    end

    struct Unbind
      getter destination : String
      getter source : String
      getter routing_key : String
      getter no_wait : Bool
      getter arguments : Amqp::Arguments

      def initialize(@destination, @source, @routing_key, @arguments, @no_wait = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_EXCHANGE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_EXCHANGE_UNBIND, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @destination)
        Types.write_shortstr(io, @source)
        Types.write_shortstr(io, @routing_key)
        BitPack.write(io, [@no_wait])
        Types.write_field_table(io, @arguments)
        io.to_slice
      end
    end

    struct UnbindOk
      def self.read(io : IO) : self
        new
      end
    end
  end
end
