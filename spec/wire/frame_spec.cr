require "../spec_helper"

describe Amqp::Wire::Frame do
  it "round-trips an empty heartbeat frame" do
    src = Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Heartbeat, 0_u16, Bytes.empty)
    io = IO::Memory.new
    src.write(io)
    io.rewind
    out = Amqp::Wire::Frame.read(io, 131_072_u32)
    out.type.should eq(Amqp::Wire::FrameType::Heartbeat)
    out.channel.should eq(0_u16)
    out.payload.size.should eq(0)
  end

  it "round-trips a method frame with payload" do
    payload = Bytes[0x00, 0x0A, 0x00, 0x0A, 0xDE, 0xAD]
    src = Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, 1_u16, payload)
    io = IO::Memory.new
    src.write(io)
    io.rewind
    out = Amqp::Wire::Frame.read(io, 131_072_u32)
    out.type.should eq(Amqp::Wire::FrameType::Method)
    out.channel.should eq(1_u16)
    out.payload.should eq(payload)
  end

  it "builds a reusable frame prefix equivalent to direct writing" do
    expected = IO::Memory.new
    Amqp::Wire::Frame.write_prefix(expected, Amqp::Wire::FrameType::Body, 7_u16, 256)

    Amqp::Wire::Frame.prefix(Amqp::Wire::FrameType::Body, 7_u16, 256).should eq(expected.to_slice)
  end

  it "raises on bad frame-end byte" do
    # METHOD type=1, channel=0, length=0, BAD end byte 0xFF
    bad = Bytes[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF]
    io = IO::Memory.new(bad)
    expect_raises(Amqp::ProtocolError, /bad frame-end/) do
      Amqp::Wire::Frame.read(io, 131_072_u32)
    end
  end

  it "raises on unknown frame type" do
    bad = Bytes[0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xCE]
    io = IO::Memory.new(bad)
    expect_raises(Amqp::ProtocolError, /unknown frame type/) do
      Amqp::Wire::Frame.read(io, 131_072_u32)
    end
  end

  it "raises when payload exceeds frame_max - 8" do
    # length = 100, but frame_max = 50 → cap-8 = 42, 100 > 42
    bad_header = IO::Memory.new
    bad_header.write_byte(0x01_u8)
    bad_header.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
    bad_header.write_bytes(100_u32, IO::ByteFormat::NetworkEndian)
    bad_header.rewind
    expect_raises(Amqp::FrameTooLargeError) do
      Amqp::Wire::Frame.read(bad_header, 50_u32)
    end
  end
end
