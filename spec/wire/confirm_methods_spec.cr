require "../spec_helper"

alias CM = Amqp::Wire::AmqpZeroNineOne::ConfirmMethods

describe Amqp::Wire::AmqpZeroNineOne::ConfirmMethods do
  it "Select encodes class=85, method=10, no-wait bit" do
    bytes = CM::Select.new(no_wait: false).to_payload
    # class_id (2) + method_id (2) + no-wait octet (1)
    bytes.size.should eq(5)
    class_id = (bytes[0].to_u16 << 8) | bytes[1].to_u16
    method_id = (bytes[2].to_u16 << 8) | bytes[3].to_u16
    class_id.should eq(85_u16)
    method_id.should eq(10_u16)
    bytes[4].should eq(0_u8)

    bytes2 = CM::Select.new(no_wait: true).to_payload
    bytes2[4].should eq(0b0000_0001_u8)
  end

  it "SelectOk.read returns an instance from empty body" do
    io = IO::Memory.new(Bytes.new(0), false)
    CM::SelectOk.read(io).should be_a(CM::SelectOk)
  end
end
