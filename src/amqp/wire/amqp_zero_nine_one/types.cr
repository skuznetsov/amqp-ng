require "../../error"
require "../../arguments"

module Amqp::Wire::AmqpZeroNineOne::Types
  extend self

  MAX_FIELD_NESTING = 32

  # ---- scalars ---------------------------------------------------------

  def read_shortstr(io : IO) : String
    len = io.read_byte
    raise IO::EOFError.new("eof reading shortstr length") if len.nil?
    return "" if len == 0
    bytes = Bytes.new(len.to_i32)
    io.read_fully(bytes)
    String.new(bytes)
  end

  def write_shortstr(io : IO, str : String) : Nil
    bytes = str.to_slice
    if bytes.size > 255
      raise Amqp::ConfigurationError.new("shortstr length #{bytes.size} exceeds 255")
    end
    io.write_byte(bytes.size.to_u8)
    io.write(bytes) if bytes.size > 0
  end

  def read_longstr_bytes(io : IO) : Bytes
    len = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
    return Bytes.empty if len == 0
    bytes = Bytes.new(len.to_i32)
    io.read_fully(bytes)
    bytes
  end

  def read_longstr_string(io : IO) : String
    String.new(read_longstr_bytes(io))
  end

  def write_longstr(io : IO, bytes : Bytes) : Nil
    io.write_bytes(bytes.size.to_u32, IO::ByteFormat::NetworkEndian)
    io.write(bytes) if bytes.size > 0
  end

  def write_longstr(io : IO, str : String) : Nil
    write_longstr(io, str.to_slice)
  end

  # ---- field-table / field-array / field-value --------------------------

  def read_field_table(io : IO, depth : Int32 = 0) : Amqp::Arguments
    guard_field_nesting!(depth, decode: true)
    total_len = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
    return Amqp::Arguments.new if total_len == 0

    body = Bytes.new(total_len.to_i32)
    io.read_fully(body)
    inner = IO::Memory.new(body)
    table = Amqp::Arguments.new
    while inner.pos < inner.size
      name = read_shortstr(inner)
      value = read_field_value(inner, depth)
      table[name] = value
    end
    table
  end

  def write_field_table(io : IO, table : Amqp::Arguments, depth : Int32 = 0) : Nil
    guard_field_nesting!(depth, decode: false)
    body = IO::Memory.new
    table.each do |name, value|
      write_shortstr(body, name)
      write_field_value(body, value, depth)
    end
    io.write_bytes(body.bytesize.to_u32, IO::ByteFormat::NetworkEndian)
    io.write(body.to_slice) if body.bytesize > 0
  end

  def read_field_array(io : IO, depth : Int32 = 0) : Array(Amqp::FieldValue)
    guard_field_nesting!(depth, decode: true)
    total_len = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
    return [] of Amqp::FieldValue if total_len == 0
    body = Bytes.new(total_len.to_i32)
    io.read_fully(body)
    inner = IO::Memory.new(body)
    arr = [] of Amqp::FieldValue
    while inner.pos < inner.size
      arr << read_field_value(inner, depth)
    end
    arr
  end

  def write_field_array(io : IO, arr : Array(Amqp::FieldValue), depth : Int32 = 0) : Nil
    guard_field_nesting!(depth, decode: false)
    body = IO::Memory.new
    arr.each { |v| write_field_value(body, v, depth) }
    io.write_bytes(body.bytesize.to_u32, IO::ByteFormat::NetworkEndian)
    io.write(body.to_slice) if body.bytesize > 0
  end

  def read_field_value(io : IO, depth : Int32 = 0) : Amqp::FieldValue
    tag = io.read_byte
    raise IO::EOFError.new("eof reading field-value tag") if tag.nil?
    case tag
    when 0x74_u8 # 't' boolean
      (io.read_byte || raise IO::EOFError.new("eof in 't'")) != 0
    when 0x62_u8 # 'b' Int8
      io.read_bytes(Int8, IO::ByteFormat::NetworkEndian)
    when 0x42_u8 # 'B' UInt8
      io.read_byte || raise IO::EOFError.new("eof in 'B'")
    when 0x73_u8 # 's' Int16
      io.read_bytes(Int16, IO::ByteFormat::NetworkEndian)
    when 0x75_u8 # 'u' UInt16
      io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
    when 0x49_u8 # 'I' Int32
      io.read_bytes(Int32, IO::ByteFormat::NetworkEndian)
    when 0x69_u8 # 'i' UInt32
      io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
    when 0x6C_u8 # 'l' Int64
      io.read_bytes(Int64, IO::ByteFormat::NetworkEndian)
    when 0x66_u8 # 'f' Float32
      io.read_bytes(Float32, IO::ByteFormat::NetworkEndian)
    when 0x64_u8 # 'd' Float64
      io.read_bytes(Float64, IO::ByteFormat::NetworkEndian)
    when 0x53_u8 # 'S' longstr (treated as String at this layer)
      read_longstr_string(io)
    when 0x41_u8 # 'A' field-array
      read_field_array(io, depth + 1)
    when 0x54_u8 # 'T' timestamp (Int64 seconds-since-epoch)
      Time.unix(io.read_bytes(Int64, IO::ByteFormat::NetworkEndian))
    when 0x46_u8 # 'F' field-table
      read_field_table(io, depth + 1)
    when 0x56_u8 # 'V' void
      nil
    when 0x78_u8 # 'x' byte-array (RabbitMQ extension)
      read_longstr_bytes(io)
    when 0x44_u8                                           # 'D' decimal — NOT SUPPORTED in v0 (see docs/05-wire-0-9-1/01-types.md §5)
      io.read_byte                                         # scale
      io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian) # value
      raise Amqp::ProtocolError.new("AMQP 'D' (decimal) field-value not supported in v0")
    else
      raise Amqp::ProtocolError.new("unknown field-value tag 0x#{tag.to_s(16)}")
    end
  end

  def write_field_value(io : IO, value : Amqp::FieldValue, depth : Int32 = 0) : Nil
    case value
    when Nil
      io.write_byte(0x56_u8) # 'V'
    when Bool
      io.write_byte(0x74_u8) # 't'
      io.write_byte(value ? 1_u8 : 0_u8)
    when Int8
      io.write_byte(0x62_u8) # 'b'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when UInt8
      io.write_byte(0x42_u8) # 'B'
      io.write_byte(value)
    when Int16
      io.write_byte(0x73_u8) # 's'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when UInt16
      io.write_byte(0x75_u8) # 'u'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when Int32
      io.write_byte(0x49_u8) # 'I'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when UInt32
      io.write_byte(0x69_u8) # 'i'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when Int64
      io.write_byte(0x6C_u8) # 'l'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when Float32
      io.write_byte(0x66_u8) # 'f'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when Float64
      io.write_byte(0x64_u8) # 'd'
      io.write_bytes(value, IO::ByteFormat::NetworkEndian)
    when Bytes
      io.write_byte(0x53_u8) # 'S' longstr (binary bytes go through 'S' too in RabbitMQ)
      write_longstr(io, value)
    when String
      io.write_byte(0x53_u8) # 'S'
      write_longstr(io, value)
    when Time
      io.write_byte(0x54_u8) # 'T'
      io.write_bytes(value.to_unix.to_i64, IO::ByteFormat::NetworkEndian)
    when Array
      io.write_byte(0x41_u8) # 'A'
      write_field_array(io, value, depth + 1)
    when Hash
      io.write_byte(0x46_u8) # 'F'
      write_field_table(io, value, depth + 1)
    else
      raise Amqp::ConfigurationError.new("unsupported field-value type #{value.class}")
    end
  end

  private def guard_field_nesting!(depth : Int32, *, decode : Bool) : Nil
    if depth > MAX_FIELD_NESTING
      message = "AMQP field nesting depth #{depth} exceeds maximum #{MAX_FIELD_NESTING}"
      raise decode ? Amqp::ProtocolError.new(message) : Amqp::ConfigurationError.new(message)
    end
  end
end
