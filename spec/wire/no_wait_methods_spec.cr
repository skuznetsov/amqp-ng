require "../spec_helper"

alias NwBasicMethods = Amqp::Wire::AmqpZeroNineOne::BasicMethods
alias NwExchangeMethods = Amqp::Wire::AmqpZeroNineOne::ExchangeMethods
alias NwQueueMethods = Amqp::Wire::AmqpZeroNineOne::QueueMethods

describe "AMQP no-wait method encoders" do
  empty_args = Amqp::Arguments.new

  it "encodes queue.declare and queue.bind no-wait bits" do
    declare = NwQueueMethods::Declare.new("q", false, true, false, false, empty_args, true).to_payload
    declare[-5].should eq(0x12_u8)

    bind = NwQueueMethods::Bind.new("q", "ex", "rk", empty_args, true).to_payload
    bind[-5].should eq(0x01_u8)
  end

  it "encodes exchange declare/bind/unbind no-wait bits" do
    declare = NwExchangeMethods::Declare.new("ex", "direct", false, true, false, false, empty_args, true).to_payload
    declare[-5].should eq(0x12_u8)

    bind = NwExchangeMethods::Bind.new("dest", "src", "rk", empty_args, true).to_payload
    bind[-5].should eq(0x01_u8)

    unbind = NwExchangeMethods::Unbind.new("dest", "src", "rk", empty_args, true).to_payload
    unbind[-5].should eq(0x01_u8)
  end

  it "encodes basic.consume and basic.cancel no-wait bits" do
    consume = NwBasicMethods::Consume.new("q", "ctag", false, true, false, empty_args, true).to_payload
    consume[-5].should eq(0x0a_u8)

    cancel = NwBasicMethods::Cancel.new("ctag", true).to_payload
    cancel[-1].should eq(0x01_u8)
  end
end
