require "../spec_helper"

alias Types = Amqp::Wire::AmqpZeroNineOne::Types

private def roundtrip_field(value : Amqp::FieldValue) : Amqp::FieldValue
  io = IO::Memory.new
  Types.write_field_value(io, value)
  io.rewind
  Types.read_field_value(io)
end

describe Amqp::Wire::AmqpZeroNineOne::Types do
  describe "shortstr" do
    it "round-trips empty and small strings" do
      ["", "x", "hello"].each do |s|
        io = IO::Memory.new
        Types.write_shortstr(io, s)
        io.rewind
        Types.read_shortstr(io).should eq(s)
      end
    end

    it "raises when shortstr exceeds 255 bytes" do
      expect_raises(Amqp::ConfigurationError, /exceeds 255/) do
        Types.write_shortstr(IO::Memory.new, "x" * 256)
      end
    end
  end

  describe "longstr" do
    it "round-trips bytes and strings" do
      io = IO::Memory.new
      payload = Bytes[0x00, 0xCE, 0xAA, 0x00]
      Types.write_longstr(io, payload)
      io.rewind
      Types.read_longstr_bytes(io).should eq(payload)
    end
  end

  describe "field_value" do
    it "round-trips Nil" do
      roundtrip_field(nil).should be_nil
    end

    it "round-trips booleans" do
      roundtrip_field(true).should eq(true)
      roundtrip_field(false).should eq(false)
    end

    it "round-trips integers of every width" do
      roundtrip_field(-1_i8).should eq(-1_i8)
      roundtrip_field(255_u8).should eq(255_u8)
      roundtrip_field(-30000_i16).should eq(-30000_i16)
      roundtrip_field(65535_u16).should eq(65535_u16)
      roundtrip_field(-1_000_000_i32).should eq(-1_000_000_i32)
      roundtrip_field(4_000_000_000_u32).should eq(4_000_000_000_u32)
      roundtrip_field(1_234_567_890_123_i64).should eq(1_234_567_890_123_i64)
    end

    it "round-trips floats" do
      roundtrip_field(3.14_f32).should eq(3.14_f32)
      roundtrip_field(3.141592653589793_f64).should eq(3.141592653589793_f64)
    end

    it "round-trips strings as 'S'" do
      roundtrip_field("hello").should eq("hello")
    end

    it "round-trips Time at second resolution" do
      t = Time.unix(1_700_000_000)
      roundtrip_field(t).should eq(t)
    end

    it "round-trips field-arrays" do
      arr = [1_i32.as(Amqp::FieldValue), "two".as(Amqp::FieldValue), true.as(Amqp::FieldValue)]
      roundtrip_field(arr).should eq(arr)
    end

    it "round-trips field-tables" do
      table = Amqp::Arguments{
        "k1" => "v1".as(Amqp::FieldValue),
        "k2" => 42_i32.as(Amqp::FieldValue),
        "k3" => true.as(Amqp::FieldValue),
      }
      roundtrip_field(table).should eq(table)
    end

    it "raises on the 'D' decimal tag" do
      io = IO::Memory.new
      io.write_byte(0x44_u8)       # 'D'
      io.write_byte(0_u8)          # scale
      io.write_bytes(0_u32, IO::ByteFormat::NetworkEndian)
      io.rewind
      expect_raises(Amqp::ProtocolError, /decimal.*not supported/) do
        Types.read_field_value(io)
      end
    end

    it "raises on unknown tag" do
      io = IO::Memory.new
      io.write_byte(0x5A_u8) # 'Z' — not assigned
      io.rewind
      expect_raises(Amqp::ProtocolError, /unknown field-value tag/) do
        Types.read_field_value(io)
      end
    end
  end

  describe "field-table" do
    it "round-trips an empty table" do
      io = IO::Memory.new
      Types.write_field_table(io, Amqp::Arguments.new)
      io.rewind
      Types.read_field_table(io).should eq(Amqp::Arguments.new)
    end
  end
end
