require "./spec_helper"
require "../src/amqp/message"

describe Amqp::Message do
  it "keeps Bytes bodies zero-copy" do
    body = Bytes[1, 2, 3]
    message = Amqp::Message.new(body)

    message.body.to_unsafe.should eq(body.to_unsafe)
  end

  it "copies String bodies into owned bytes" do
    source = String.new(Bytes[0x61, 0x62, 0x63])
    message = Amqp::Message.new(source)

    message.body.should eq(source.to_slice)
    message.body.to_unsafe.should_not eq(source.to_slice.to_unsafe)
  end

  it "copies IO bodies into owned bytes" do
    source = String.new(Bytes[0x64, 0x65, 0x66])
    io = IO::Memory.new(source.to_slice, writeable: false)
    message = Amqp::Message.new(io)

    message.body.should eq(source.to_slice)
    message.body.to_unsafe.should_not eq(source.to_slice.to_unsafe)
  end
end
