require "../../error"
require "./types"
require "./connection_methods"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_CHANNEL_OPEN     = 10_u16
  METHOD_ID_CHANNEL_OPEN_OK  = 11_u16
  METHOD_ID_CHANNEL_FLOW     = 20_u16
  METHOD_ID_CHANNEL_FLOW_OK  = 21_u16
  METHOD_ID_CHANNEL_CLOSE    = 40_u16
  METHOD_ID_CHANNEL_CLOSE_OK = 41_u16

  module ChannelMethods
    extend self

    private def header(io : IO, method_id : UInt16)
      io.write_bytes(CLASS_ID_CHANNEL, IO::ByteFormat::NetworkEndian)
      io.write_bytes(method_id, IO::ByteFormat::NetworkEndian)
    end

    struct Open
      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_CHANNEL, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_CHANNEL_OPEN, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, "")
        io.to_slice
      end
    end

    struct OpenOk
      def self.read(io : IO) : self
        # reserved-1 longstr; ignore
        len = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
        io.skip(len.to_i32) if len > 0
        new
      end
    end

    struct Flow
      getter active : Bool

      def initialize(@active)
      end

      def self.read(io : IO) : self
        b = io.read_byte || raise Amqp::ProtocolError.new("eof in channel.flow")
        new((b & 0x01) != 0)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_CHANNEL, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_CHANNEL_FLOW, IO::ByteFormat::NetworkEndian)
        io.write_byte(@active ? 1_u8 : 0_u8)
        io.to_slice
      end
    end

    struct FlowOk
      getter active : Bool

      def initialize(@active)
      end

      def self.read(io : IO) : self
        b = io.read_byte || raise Amqp::ProtocolError.new("eof in channel.flow-ok")
        new((b & 0x01) != 0)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_CHANNEL, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_CHANNEL_FLOW_OK, IO::ByteFormat::NetworkEndian)
        io.write_byte(@active ? 1_u8 : 0_u8)
        io.to_slice
      end
    end

    struct Close
      getter reply_code : UInt16
      getter reply_text : String
      getter class_id : UInt16
      getter method_id : UInt16

      def initialize(@reply_code, @reply_text, @class_id, @method_id)
      end

      def self.read(io : IO) : self
        rc = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        rt = Types.read_shortstr(io)
        cid = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        mid = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        new(rc, rt, cid, mid)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_CHANNEL, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_CHANNEL_CLOSE, IO::ByteFormat::NetworkEndian)
        io.write_bytes(@reply_code, IO::ByteFormat::NetworkEndian)
        Types.write_shortstr(io, @reply_text)
        io.write_bytes(@class_id, IO::ByteFormat::NetworkEndian)
        io.write_bytes(@method_id, IO::ByteFormat::NetworkEndian)
        io.to_slice
      end
    end

    struct CloseOk
      def self.read(io : IO) : self
        new
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_CHANNEL, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_CHANNEL_CLOSE_OK, IO::ByteFormat::NetworkEndian)
        io.to_slice
      end
    end
  end
end
