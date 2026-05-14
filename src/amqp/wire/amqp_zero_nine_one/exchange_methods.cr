require "../../error"
require "./types"
require "./bit_pack"
require "./connection_methods"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_EXCHANGE_DECLARE    = 10_u16
  METHOD_ID_EXCHANGE_DECLARE_OK = 11_u16
  METHOD_ID_EXCHANGE_DELETE     = 20_u16
  METHOD_ID_EXCHANGE_DELETE_OK  = 21_u16

  module ExchangeMethods
    extend self

    struct Declare
      getter name : String
      getter type : String
      getter passive : Bool
      getter durable : Bool
      getter auto_delete : Bool
      getter internal : Bool
      getter arguments : Amqp::Arguments

      def initialize(@name, @type, @passive, @durable, @auto_delete, @internal,
                     @arguments)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_EXCHANGE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_EXCHANGE_DECLARE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian) # reserved-1
        Types.write_shortstr(io, @name)
        Types.write_shortstr(io, @type)
        BitPack.write(io, [@passive, @durable, @auto_delete, @internal, false])
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

      def initialize(@name, @if_unused)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_EXCHANGE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_EXCHANGE_DELETE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @name)
        BitPack.write(io, [@if_unused, false])
        io.to_slice
      end
    end

    struct DeleteOk
      def self.read(io : IO) : self
        new
      end
    end
  end
end
