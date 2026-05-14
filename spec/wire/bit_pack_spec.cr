require "../spec_helper"

alias BP = Amqp::Wire::AmqpZeroNineOne::BitPack

describe Amqp::Wire::AmqpZeroNineOne::BitPack do
  it "packs 5 bits LSB-first into a single octet" do
    io = IO::Memory.new
    BP.write(io, [true, false, true, false, true])
    io.to_slice.should eq(Bytes[0b00010101_u8])
  end

  it "round-trips up to 16 bits across 2 octets" do
    bits = [true, false, true, true, false, false, true, false,
            false, true, false, false, true, false, true, false]
    io = IO::Memory.new
    BP.write(io, bits)
    io.rewind
    BP.read(io, bits.size).should eq(bits)
  end
end
