require "../error"

module Amqp::Wire
  FRAME_END = 0xCE_u8

  enum FrameType : UInt8
    Method    = 1
    Header    = 2
    Body      = 3
    Heartbeat = 8
  end

  struct Frame
    getter type : FrameType
    getter channel : UInt16
    getter payload : Bytes

    def initialize(@type : FrameType, @channel : UInt16, @payload : Bytes)
    end

    # frame_max=0 means "no limit" per AMQP 0-9-1; the codec applies a
    # 128 KiB ceiling in that case (per docs/05-wire-0-9-1/00-frames.md §5).
    def self.read(io : IO, frame_max : UInt32) : Frame
      type_byte = io.read_byte
      raise IO::EOFError.new("eof reading frame type") if type_byte.nil?
      type = FrameType.from_value?(type_byte)
      raise Amqp::ProtocolError.new("unknown frame type 0x#{type_byte.to_s(16)}") if type.nil?

      channel = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
      length = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)

      cap = frame_max == 0 ? 131_072_u32 : frame_max
      if length > cap - 8
        raise Amqp::FrameTooLargeError.new("frame length #{length} exceeds frame_max-8 #{cap - 8}")
      end

      payload = Bytes.new(length.to_i32)
      io.read_fully(payload) if length > 0

      end_byte = io.read_byte
      raise IO::EOFError.new("eof reading frame end") if end_byte.nil?
      unless end_byte == FRAME_END
        raise Amqp::ProtocolError.new("bad frame-end byte 0x#{end_byte.to_s(16)}, expected 0xCE")
      end

      new(type, channel, payload)
    end

    def write(io : IO) : Nil
      Frame.write_prefix(io, @type, @channel, @payload.size)
      io.write(@payload) if @payload.size > 0
      io.write_byte(FRAME_END)
    end

    def self.write_prefix(io : IO, type : FrameType, channel : UInt16, payload_size : Int) : Nil
      io.write_byte(type.value)
      io.write_bytes(channel, IO::ByteFormat::NetworkEndian)
      io.write_bytes(payload_size.to_u32, IO::ByteFormat::NetworkEndian)
    end
  end
end
