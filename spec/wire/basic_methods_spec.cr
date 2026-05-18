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
end
