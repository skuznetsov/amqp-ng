require "../../error"
require "../frame"
require "./types"
require "./bit_pack"
require "./connection_methods"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_BASIC_QOS        =  10_u16
  METHOD_ID_BASIC_QOS_OK     =  11_u16
  METHOD_ID_BASIC_CONSUME    =  20_u16
  METHOD_ID_BASIC_CONSUME_OK =  21_u16
  METHOD_ID_BASIC_CANCEL     =  30_u16
  METHOD_ID_BASIC_CANCEL_OK  =  31_u16
  METHOD_ID_BASIC_PUBLISH    =  40_u16
  METHOD_ID_BASIC_RETURN     =  50_u16
  METHOD_ID_BASIC_DELIVER    =  60_u16
  METHOD_ID_BASIC_GET        =  70_u16
  METHOD_ID_BASIC_GET_OK     =  71_u16
  METHOD_ID_BASIC_GET_EMPTY  =  72_u16
  METHOD_ID_BASIC_ACK        =  80_u16
  METHOD_ID_BASIC_REJECT     =  90_u16
  METHOD_ID_BASIC_RECOVER    = 110_u16
  METHOD_ID_BASIC_RECOVER_OK = 111_u16
  METHOD_ID_BASIC_NACK       = 120_u16

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
      getter no_wait : Bool
      getter arguments : Amqp::Arguments

      def initialize(@queue, @consumer_tag, @no_local, @no_ack, @exclusive,
                     @arguments, @no_wait = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_CONSUME, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian) # reserved-1
        Types.write_shortstr(io, @queue)
        Types.write_shortstr(io, @consumer_tag)
        BitPack.write(io, [@no_local, @no_ack, @exclusive, @no_wait])
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
      getter no_wait : Bool

      def initialize(@consumer_tag, @no_wait = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_CANCEL, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @consumer_tag)
        BitPack.write(io, [@no_wait])
        io.to_slice
      end

      def self.read(io : IO) : self
        # Server-initiated cancel: consumer-tag + no-wait
        tag = Types.read_shortstr(io)
        b = io.read_byte || raise Amqp::ProtocolError.new("eof in basic.cancel")
        new(tag, (b & 0x01) != 0)
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
      getter immediate : Bool

      def initialize(@exchange, @routing_key, @mandatory, @immediate = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_PUBLISH, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian) # reserved-1
        Types.write_shortstr(io, @exchange)
        Types.write_shortstr(io, @routing_key)
        BitPack.write(io, [@mandatory, @immediate])
        io.to_slice
      end
    end

    def write_publish_frame(io : IO,
                            channel : UInt16,
                            exchange : String,
                            routing_key : String,
                            mandatory : Bool,
                            immediate : Bool = false) : Nil
      exchange_bytes = exchange.to_slice
      routing_key_bytes = routing_key.to_slice
      if exchange_bytes.size > 255
        raise Amqp::ConfigurationError.new("shortstr length #{exchange_bytes.size} exceeds 255")
      end
      if routing_key_bytes.size > 255
        raise Amqp::ConfigurationError.new("shortstr length #{routing_key_bytes.size} exceeds 255")
      end

      payload_size = 9 + exchange_bytes.size + routing_key_bytes.size
      Frame.write_prefix(io, FrameType::Method, channel, payload_size)
      io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
      io.write_bytes(METHOD_ID_BASIC_PUBLISH, IO::ByteFormat::NetworkEndian)
      io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
      io.write_byte(exchange_bytes.size.to_u8)
      io.write(exchange_bytes) if exchange_bytes.size > 0
      io.write_byte(routing_key_bytes.size.to_u8)
      io.write(routing_key_bytes) if routing_key_bytes.size > 0
      bits = 0_u8
      bits |= 1_u8 if mandatory
      bits |= 2_u8 if immediate
      io.write_byte(bits)
      io.write_byte(Amqp::Wire::FRAME_END)
    end

    def publish_frame(channel : UInt16,
                      exchange : String,
                      routing_key : String,
                      mandatory : Bool,
                      immediate : Bool = false) : Bytes
      io = IO::Memory.new
      write_publish_frame(io, channel, exchange, routing_key, mandatory, immediate)
      io.to_slice
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

    def decode_deliver_frame_payload(payload : Bytes) : Deliver?
      return nil unless payload.size >= 16
      return nil unless read_u16_be(payload, 0) == CLASS_ID_BASIC
      return nil unless read_u16_be(payload, 2) == METHOD_ID_BASIC_DELIVER

      offset = 4
      tag = read_shortstr_direct(payload, offset) || return nil
      offset += 1 + tag.bytesize
      return nil if payload.size < offset + 9

      delivery_tag = read_u64_be(payload, offset)
      offset += 8
      redelivered = (payload[offset] & 0x01) != 0
      offset += 1

      exchange = read_shortstr_direct(payload, offset) || return nil
      offset += 1 + exchange.bytesize
      routing_key = read_shortstr_direct(payload, offset) || return nil
      offset += 1 + routing_key.bytesize
      return nil unless offset == payload.size

      Deliver.new(tag, delivery_tag, redelivered, exchange, routing_key)
    end

    struct Get
      getter queue : String
      getter no_ack : Bool

      def initialize(@queue, @no_ack)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_GET, IO::ByteFormat::NetworkEndian)
        io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @queue)
        BitPack.write(io, [@no_ack])
        io.to_slice
      end
    end

    struct GetOk
      getter delivery_tag : UInt64
      getter redelivered : Bool
      getter exchange : String
      getter routing_key : String
      getter message_count : UInt32

      def initialize(@delivery_tag, @redelivered, @exchange, @routing_key, @message_count)
      end

      def self.read(io : IO) : self
        dtag = io.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
        b = io.read_byte || raise Amqp::ProtocolError.new("eof in basic.get-ok")
        ex = Types.read_shortstr(io)
        rk = Types.read_shortstr(io)
        count = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
        new(dtag, (b & 0x01) != 0, ex, rk, count)
      end
    end

    struct GetEmpty
      def self.read(io : IO) : self
        Types.read_shortstr(io) # reserved-1
        new
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

    def write_ack_frame(io : IO,
                        channel : UInt16,
                        delivery_tag : UInt64,
                        multiple : Bool) : Nil
      write_delivery_settlement_frame(io, channel, METHOD_ID_BASIC_ACK,
        delivery_tag, multiple ? 1_u8 : 0_u8)
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

    def write_nack_frame(io : IO,
                         channel : UInt16,
                         delivery_tag : UInt64,
                         multiple : Bool,
                         requeue : Bool) : Nil
      bits = 0_u8
      bits |= 1_u8 if multiple
      bits |= 2_u8 if requeue
      write_delivery_settlement_frame(io, channel, METHOD_ID_BASIC_NACK, delivery_tag, bits)
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

    def write_reject_frame(io : IO,
                           channel : UInt16,
                           delivery_tag : UInt64,
                           requeue : Bool) : Nil
      write_delivery_settlement_frame(io, channel, METHOD_ID_BASIC_REJECT,
        delivery_tag, requeue ? 1_u8 : 0_u8)
    end

    private def write_delivery_settlement_frame(io : IO,
                                                channel : UInt16,
                                                method_id : UInt16,
                                                delivery_tag : UInt64,
                                                bits : UInt8) : Nil
      Frame.write_prefix(io, FrameType::Method, channel, 13)
      io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
      io.write_bytes(method_id, IO::ByteFormat::NetworkEndian)
      io.write_bytes(delivery_tag, IO::ByteFormat::NetworkEndian)
      io.write_byte(bits)
      io.write_byte(Amqp::Wire::FRAME_END)
    end

    private def read_shortstr_direct(bytes : Bytes, offset : Int32) : String?
      return nil if offset >= bytes.size

      size = bytes[offset].to_i32
      start = offset + 1
      return nil if bytes.size < start + size

      String.new(bytes[start, size])
    end

    private def read_u16_be(bytes : Bytes, offset : Int32) : UInt16
      ((bytes[offset].to_u16 << 8) | bytes[offset + 1].to_u16).to_u16
    end

    private def read_u64_be(bytes : Bytes, offset : Int32) : UInt64
      value = 0_u64
      8.times do |i|
        value = (value << 8) | bytes[offset + i].to_u64
      end
      value
    end

    struct Recover
      getter requeue : Bool

      def initialize(@requeue)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_BASIC_RECOVER, IO::ByteFormat::NetworkEndian)
        BitPack.write(io, [@requeue])
        io.to_slice
      end
    end

    struct RecoverOk
      def self.read(io : IO) : self
        new
      end
    end
  end
end
