require "../spec_helper"

alias CH = Amqp::Wire::AmqpZeroNineOne::ContentHeader

describe Amqp::Wire::AmqpZeroNineOne::ContentHeader do
  it "empty Properties → 14-byte header payload, flags=0" do
    bytes = CH.encode(60_u16, 0_u64, Amqp::Properties.new)
    bytes.size.should eq(14)
    # last 2 bytes are flags
    (bytes[12].to_u16 << 8 | bytes[13].to_u16).should eq(0_u16)
  end

  it "write_empty_frame matches the generic frame encoder for empty properties" do
    payload = CH.encode(60_u16, 123_u64, Amqp::Properties.new)
    expected = IO::Memory.new
    Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Header, 9_u16, payload).write(expected)

    actual = IO::Memory.new
    CH.write_empty_frame(actual, 9_u16, 60_u16, 123_u64)

    actual.to_slice.should eq(expected.to_slice)
  end

  it "builds a reusable empty content-header frame equivalent to direct writing" do
    expected = IO::Memory.new
    CH.write_empty_frame(expected, 9_u16, 60_u16, 123_u64)

    CH.empty_frame(9_u16, 60_u16, 123_u64).should eq(expected.to_slice)
  end

  it "reuses encoded property tails across body sizes" do
    props = Amqp::Properties.new(
      content_type: "text/plain",
      delivery_mode: Amqp::Properties::Persistence::Persistent,
    )
    encoded = CH.encode_properties(props)

    {0_u64, 123_u64}.each do |body_size|
      expected = IO::Memory.new
      Amqp::Wire::Frame.new(
        Amqp::Wire::FrameType::Header,
        9_u16,
        CH.encode(60_u16, body_size, props),
      ).write(expected)

      actual = IO::Memory.new
      CH.write_frame(actual, 9_u16, 60_u16, body_size, encoded)

      actual.to_slice.should eq(expected.to_slice)
      CH.frame(9_u16, 60_u16, body_size, encoded).should eq(expected.to_slice)
    end
  end

  it "direct-decodes an empty content-header payload equivalent to generic decode" do
    payload = CH.encode(60_u16, 123_u64, Amqp::Properties.new)
    direct = CH.decode_empty(payload)
    generic = CH.decode(payload)

    direct.should_not be_nil
    direct = direct.not_nil!
    direct.class_id.should eq(generic.class_id)
    direct.body_size.should eq(generic.body_size)
    direct.properties.empty?.should be_true
  end

  it "does not direct-decode non-empty properties or malformed empty headers" do
    payload = CH.encode(60_u16, 123_u64, Amqp::Properties.new(content_type: "text/plain"))
    CH.decode_empty(payload).should be_nil

    malformed = CH.encode(60_u16, 123_u64, Amqp::Properties.new)
    malformed[3] = 1_u8
    CH.decode_empty(malformed).should be_nil
  end

  it "content-type + persistent → flags 0x9000" do
    props = Amqp::Properties.new(
      content_type: "text/plain",
      delivery_mode: Amqp::Properties::Persistence::Persistent,
    )
    bytes = CH.encode(60_u16, 5_u64, props)
    flags = (bytes[12].to_u16 << 8) | bytes[13].to_u16
    flags.should eq(0x9000_u16)
  end

  it "round-trips a representative property set" do
    props = Amqp::Properties.new(
      content_type: "application/json",
      content_encoding: "gzip",
      headers: Amqp::Arguments{
        "x-priority" => 7_i32.as(Amqp::FieldValue),
        "x-tag"      => "hello".as(Amqp::FieldValue),
      },
      delivery_mode: Amqp::Properties::Persistence::Persistent,
      priority: 5_u8,
      correlation_id: "corr-1",
      reply_to: "amqp-ng.reply",
      expiration: "60000",
      message_id: "msg-42",
      timestamp: Time.unix(1_700_000_000),
      type: "demo",
      user_id: "guest",
      app_id: "amqp-ng-test",
    )
    body = "hello world".to_slice
    encoded = CH.encode(60_u16, body.size.to_u64, props)
    decoded = CH.decode(encoded)
    decoded.class_id.should eq(60_u16)
    decoded.body_size.should eq(body.size.to_u64)
    decoded.properties.content_type.should eq("application/json")
    decoded.properties.content_encoding.should eq("gzip")
    decoded.properties.delivery_mode.should eq(Amqp::Properties::Persistence::Persistent)
    decoded.properties.priority.should eq(5_u8)
    decoded.properties.correlation_id.should eq("corr-1")
    decoded.properties.reply_to.should eq("amqp-ng.reply")
    decoded.properties.expiration.should eq("60000")
    decoded.properties.message_id.should eq("msg-42")
    decoded.properties.timestamp.should eq(Time.unix(1_700_000_000))
    decoded.properties.type.should eq("demo")
    decoded.properties.user_id.should eq("guest")
    decoded.properties.app_id.should eq("amqp-ng-test")
    decoded.properties.headers.should eq(props.headers)
  end

  it "rejects weight != 0 on decode" do
    raw = IO::Memory.new
    raw.write_bytes(60_u16, IO::ByteFormat::NetworkEndian)
    raw.write_bytes(1_u16, IO::ByteFormat::NetworkEndian) # bad weight
    raw.write_bytes(0_u64, IO::ByteFormat::NetworkEndian)
    raw.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
    expect_raises(Amqp::ProtocolError, /weight/) do
      CH.decode(raw.to_slice)
    end
  end

  it "rejects continuation bit on decode" do
    raw = IO::Memory.new
    raw.write_bytes(60_u16, IO::ByteFormat::NetworkEndian)
    raw.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
    raw.write_bytes(0_u64, IO::ByteFormat::NetworkEndian)
    raw.write_bytes(0x0001_u16, IO::ByteFormat::NetworkEndian) # continuation set
    expect_raises(Amqp::ProtocolError, /continuation/) do
      CH.decode(raw.to_slice)
    end
  end

  it "rejects non-ASCII-digit expiration on encode" do
    expect_raises(Amqp::ConfigurationError, /ASCII decimal/) do
      CH.encode(60_u16, 0_u64, Amqp::Properties.new(expiration: "60s"))
    end
  end
end
