require "../../error"
require "./types"
require "./bit_pack"
require "./connection_methods"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_QUEUE_DECLARE    = 10_u16
  METHOD_ID_QUEUE_DECLARE_OK = 11_u16
  METHOD_ID_QUEUE_BIND       = 20_u16
  METHOD_ID_QUEUE_BIND_OK    = 21_u16
  METHOD_ID_QUEUE_PURGE      = 30_u16
  METHOD_ID_QUEUE_PURGE_OK   = 31_u16
  METHOD_ID_QUEUE_DELETE     = 40_u16
  METHOD_ID_QUEUE_DELETE_OK  = 41_u16
  METHOD_ID_QUEUE_UNBIND     = 50_u16
  METHOD_ID_QUEUE_UNBIND_OK  = 51_u16

  module QueueMethods
    extend self

    struct Declare
      getter name : String
      getter passive : Bool
      getter durable : Bool
      getter exclusive : Bool
      getter auto_delete : Bool
      getter arguments : Amqp::Arguments

      def initialize(@name, @passive, @durable, @exclusive, @auto_delete,
                     @arguments)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_QUEUE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_QUEUE_DECLARE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian) # reserved-1
        Types.write_shortstr(io, @name)
        BitPack.write(io, [@passive, @durable, @exclusive, @auto_delete, false])
        Types.write_field_table(io, @arguments)
        io.to_slice
      end
    end

    struct DeclareOk
      getter name : String
      getter message_count : UInt32
      getter consumer_count : UInt32

      def initialize(@name, @message_count, @consumer_count)
      end

      def self.read(io : IO) : self
        name = Types.read_shortstr(io)
        msgs = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
        cons = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
        new(name, msgs, cons)
      end
    end

    struct Bind
      getter queue : String
      getter exchange : String
      getter routing_key : String
      getter arguments : Amqp::Arguments

      def initialize(@queue, @exchange, @routing_key, @arguments)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_QUEUE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_QUEUE_BIND, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @queue)
        Types.write_shortstr(io, @exchange)
        Types.write_shortstr(io, @routing_key)
        BitPack.write(io, [false]) # no-wait
        Types.write_field_table(io, @arguments)
        io.to_slice
      end
    end

    struct BindOk
      def self.read(io : IO) : self
        new
      end
    end

    struct Purge
      getter name : String

      def initialize(@name)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_QUEUE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_QUEUE_PURGE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @name)
        BitPack.write(io, [false]) # no-wait
        io.to_slice
      end
    end

    struct PurgeOk
      getter message_count : UInt32

      def initialize(@message_count)
      end

      def self.read(io : IO) : self
        new(io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian))
      end
    end

    struct Delete
      getter name : String
      getter if_unused : Bool
      getter if_empty : Bool

      def initialize(@name, @if_unused, @if_empty)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_QUEUE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_QUEUE_DELETE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @name)
        BitPack.write(io, [@if_unused, @if_empty, false])
        io.to_slice
      end
    end

    struct DeleteOk
      getter message_count : UInt32

      def initialize(@message_count)
      end

      def self.read(io : IO) : self
        new(io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian))
      end
    end

    struct Unbind
      getter queue : String
      getter exchange : String
      getter routing_key : String
      getter arguments : Amqp::Arguments

      def initialize(@queue, @exchange, @routing_key, @arguments)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_QUEUE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_QUEUE_UNBIND, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @queue)
        Types.write_shortstr(io, @exchange)
        Types.write_shortstr(io, @routing_key)
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
