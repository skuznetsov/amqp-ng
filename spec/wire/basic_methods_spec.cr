require "../spec_helper"

alias BM = Amqp::Wire::AmqpZeroNineOne::BasicMethods

describe Amqp::Wire::AmqpZeroNineOne::BasicMethods do
  it "encodes basic.recover and decodes recover-ok" do
    payload = BM::Recover.new(requeue: true).to_payload
    payload.should eq(Bytes[
      0x00, 0x3c, # class basic
      0x00, 0x6e, # method recover
      0x01,       # requeue bit
    ])
    BM::RecoverOk.read(IO::Memory.new).should be_a(BM::RecoverOk)
  end

  describe ".write_publish_frame" do
    it "matches the generic frame encoder for basic.publish" do
      channel = 7_u16
      payload = BM::Publish.new("events", "jobs.created", mandatory: true, immediate: false).to_payload
      expected = IO::Memory.new
      Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, channel, payload).write(expected)

      actual = IO::Memory.new
      BM.write_publish_frame(actual, channel, "events", "jobs.created", mandatory: true, immediate: false)

      actual.to_slice.should eq(expected.to_slice)
    end

    it "matches the generic frame encoder for empty exchange and immediate bit" do
      channel = 1_u16
      payload = BM::Publish.new("", "queue", mandatory: false, immediate: true).to_payload
      expected = IO::Memory.new
      Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, channel, payload).write(expected)

      actual = IO::Memory.new
      BM.write_publish_frame(actual, channel, "", "queue", mandatory: false, immediate: true)

      actual.to_slice.should eq(expected.to_slice)
    end

    it "rejects oversized short strings before writing a partial frame" do
      io = IO::Memory.new
      expect_raises(Amqp::ConfigurationError, /shortstr/) do
        BM.write_publish_frame(io, 1_u16, "x" * 256, "queue", mandatory: false)
      end
      io.to_slice.should be_empty
    end

    it "builds a reusable publish method frame equivalent to direct writing" do
      expected = IO::Memory.new
      BM.write_publish_frame(expected, 3_u16, "", "jobs", mandatory: false)

      BM.publish_frame(3_u16, "", "jobs", mandatory: false).should eq(expected.to_slice)
    end
  end

  describe "direct delivery settlement writers" do
    it "write_ack_frame matches the generic frame encoder" do
      payload = BM::Ack.new(42_u64, true).to_payload
      expected = IO::Memory.new
      Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, 5_u16, payload).write(expected)

      actual = IO::Memory.new
      BM.write_ack_frame(actual, 5_u16, 42_u64, true)

      actual.to_slice.should eq(expected.to_slice)
    end

    it "write_nack_frame matches the generic frame encoder" do
      payload = BM::Nack.new(42_u64, true, false).to_payload
      expected = IO::Memory.new
      Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, 5_u16, payload).write(expected)

      actual = IO::Memory.new
      BM.write_nack_frame(actual, 5_u16, 42_u64, true, false)

      actual.to_slice.should eq(expected.to_slice)
    end

    it "write_reject_frame matches the generic frame encoder" do
      payload = BM::Reject.new(42_u64, true).to_payload
      expected = IO::Memory.new
      Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, 5_u16, payload).write(expected)

      actual = IO::Memory.new
      BM.write_reject_frame(actual, 5_u16, 42_u64, true)

      actual.to_slice.should eq(expected.to_slice)
    end
  end

  describe ".decode_deliver_frame_payload" do
    it "decodes a basic.deliver method payload equivalent to the generic reader" do
      payload = IO::Memory.new
      payload.write_bytes(Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
      payload.write_bytes(Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_DELIVER, IO::ByteFormat::NetworkEndian)
      Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(payload, "ctag-1")
      payload.write_bytes(42_u64, IO::ByteFormat::NetworkEndian)
      payload.write_byte(1_u8)
      Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(payload, "")
      Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(payload, "queue-a")
      bytes = payload.to_slice

      generic = BM::Deliver.read(IO::Memory.new(bytes[4, bytes.size - 4], false))
      direct = BM.decode_deliver_frame_payload(bytes)

      direct.should_not be_nil
      direct = direct.not_nil!
      direct.consumer_tag.should eq(generic.consumer_tag)
      direct.delivery_tag.should eq(generic.delivery_tag)
      direct.redelivered.should eq(generic.redelivered)
      direct.exchange.should eq(generic.exchange)
      direct.routing_key.should eq(generic.routing_key)
    end

    it "falls back on non-deliver or truncated deliver payloads" do
      BM.decode_deliver_frame_payload(BM::Ack.new(1_u64, false).to_payload).should be_nil

      payload = IO::Memory.new
      payload.write_bytes(Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC, IO::ByteFormat::NetworkEndian)
      payload.write_bytes(Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_DELIVER, IO::ByteFormat::NetworkEndian)
      payload.write_byte(10_u8)
      payload.write("short".to_slice)
      BM.decode_deliver_frame_payload(payload.to_slice).should be_nil
    end
  end
end
