require "../../error"
require "./types"
require "./bit_pack"
require "./connection_methods"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_BASIC_QOS          = 10_u16
  METHOD_ID_BASIC_QOS_OK       = 11_u16
  METHOD_ID_BASIC_CONSUME      = 20_u16
  METHOD_ID_BASIC_CONSUME_OK   = 21_u16
  METHOD_ID_BASIC_CANCEL       = 30_u16
  METHOD_ID_BASIC_CANCEL_OK    = 31_u16
  METHOD_ID_BASIC_PUBLISH      = 40_u16
  METHOD_ID_BASIC_RETURN       = 50_u16
  METHOD_ID_BASIC_DELIVER      = 60_u16
  METHOD_ID_BASIC_GET          = 70_u16
  METHOD_ID_BASIC_GET_OK       = 71_u16
  METHOD_ID_BASIC_GET_EMPTY    = 72_u16
  METHOD_ID_BASIC_ACK          = 80_u16
  METHOD_ID_BASIC_REJECT       = 90_u16
  METHOD_ID_BASIC_NACK         = 120_u16

  module BasicMethods
    extend self

    struct Qos
      getter prefetch_size : UInt32
      getter prefetch_count : UInt16
      getter global : Bool

      def initialize(@prefetch_size, @prefetch_count, @global)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_QOS, IO::ByteFormat::NetworkEndian)
        io.write_bytes(@prefetch_size, IO::ByteFormat::NetworkEndian)
        io.write_bytes(@prefetch_count, IO::ByteFormat::NetworkEndian)
        BitPack.write(io, [@global])
        io.to_slice
      end
    end

    struct QosOk
      def self.read(io : IO) : self
        new
      end
    end

    struct Consume
      getter queue : String
      getter consumer_tag : String
      getter no_local : Bool
      getter no_ack : Bool
      getter exclusive : Bool
      getter arguments : Amqp::Arguments

      def initialize(@queue, @consumer_tag, @no_local, @no_ack, @exclusive,
                     @arguments)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_CONSUME, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian) # reserved-1
        Types.write_shortstr(io, @queue)
        Types.write_shortstr(io, @consumer_tag)
        BitPack.write(io, [@no_local, @no_ack, @exclusive, false]) # no-wait=false
        Types.write_field_table(io, @arguments)
        io.to_slice
      end
    end

    struct ConsumeOk
      getter consumer_tag : String

      def initialize(@consumer_tag)
      end

      def self.read(io : IO) : self
        new(Types.read_shortstr(io))
      end
    end

    struct Cancel
      getter consumer_tag : String

      def initialize(@consumer_tag)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_CANCEL, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @consumer_tag)
        BitPack.write(io, [false]) # no-wait
        io.to_slice
      end

      def self.read(io : IO) : self
        # Server-initiated cancel: consumer-tag + no-wait
        tag = Types.read_shortstr(io)
        io.read_byte # no-wait, ignore
        new(tag)
      end
    end

    struct CancelOk
      getter consumer_tag : String

      def initialize(@consumer_tag)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_CANCEL_OK, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @consumer_tag)
        io.to_slice
      end

      def self.read(io : IO) : self
        new(Types.read_shortstr(io))
      end
    end

    struct Publish
      getter exchange : String
      getter routing_key : String
      getter mandatory : Bool

      def initialize(@exchange, @routing_key, @mandatory)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_PUBLISH, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian) # reserved-1
        Types.write_shortstr(io, @exchange)
        Types.write_shortstr(io, @routing_key)
        BitPack.write(io, [@mandatory, false]) # immediate=false
        io.to_slice
      end
    end

    struct Return
      getter reply_code : UInt16
      getter reply_text : String
      getter exchange : String
      getter routing_key : String

      def initialize(@reply_code, @reply_text, @exchange, @routing_key)
      end

      def self.read(io : IO) : self
        rc = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        rt = Types.read_shortstr(io)
        ex = Types.read_shortstr(io)
        rk = Types.read_shortstr(io)
        new(rc, rt, ex, rk)
      end
    end

    struct Deliver
      getter consumer_tag : String
      getter delivery_tag : UInt64
      getter redelivered : Bool
      getter exchange : String
      getter routing_key : String

      def initialize(@consumer_tag, @delivery_tag, @redelivered, @exchange,
                     @routing_key)
      end

      def self.read(io : IO) : self
        tag = Types.read_shortstr(io)
        dtag = io.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
        b = io.read_byte || raise Amqp::ProtocolError.new("eof in basic.deliver")
        rd = (b & 0x01) != 0
        ex = Types.read_shortstr(io)
        rk = Types.read_shortstr(io)
        new(tag, dtag, rd, ex, rk)
      end
    end

    struct Ack
      getter delivery_tag : UInt64
      getter multiple : Bool

      def initialize(@delivery_tag, @multiple)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_ACK, IO::ByteFormat::NetworkEndian)
        io.write_bytes(@delivery_tag, IO::ByteFormat::NetworkEndian)
        BitPack.write(io, [@multiple])
        io.to_slice
      end

      def self.read(io : IO) : self
        dt = io.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
        b = io.read_byte || raise Amqp::ProtocolError.new("eof in basic.ack")
        new(dt, (b & 0x01) != 0)
      end
    end

    struct Nack
      getter delivery_tag : UInt64
      getter multiple : Bool
      getter requeue : Bool

      def initialize(@delivery_tag, @multiple, @requeue)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_NACK, IO::ByteFormat::NetworkEndian)
        io.write_bytes(@delivery_tag, IO::ByteFormat::NetworkEndian)
        BitPack.write(io, [@multiple, @requeue])
        io.to_slice
      end

      def self.read(io : IO) : self
        dt = io.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
        b = io.read_byte || raise Amqp::ProtocolError.new("eof in basic.nack")
        new(dt, (b & 0x01) != 0, (b & 0x02) != 0)
      end
    end

    struct Reject
      getter delivery_tag : UInt64
      getter requeue : Bool

      def initialize(@delivery_tag, @requeue)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_REJECT, IO::ByteFormat::NetworkEndian)
        io.write_bytes(@delivery_tag, IO::ByteFormat::NetworkEndian)
        BitPack.write(io, [@requeue])
        io.to_slice
      end
    end
  end
end
