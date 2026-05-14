module Amqp::Wire::AmqpZeroNineOne
  # Packs/unpacks consecutive bit fields into octets, LSB-first.
  # Used by exchange.declare, queue.declare, basic.publish, etc.
  module BitPack
    extend self

    # Writes the given bools as a sequence of octets, LSB-first.
    # AMQP packs runs of consecutive bit fields into the smallest number
    # of octets; this helper does the whole run at once.
    def write(io : IO, bits : Array(Bool)) : Nil
      i = 0
      while i < bits.size
        byte = 0_u8
        bit = 0
        while bit < 8 && i + bit < bits.size
          byte |= 1_u8 << bit if bits[i + bit]
          bit += 1
        end
        io.write_byte(byte)
        i += 8
      end
    end

    # Reads `count` bits packed LSB-first into ceil(count/8) octets.
    def read(io : IO, count : Int32) : Array(Bool)
      bytes_needed = (count + 7) // 8
      bits = Array(Bool).new(count, false)
      bytes_needed.times do |b|
        byte = io.read_byte || raise Amqp::ProtocolError.new("eof reading packed bits")
        8.times do |bit|
          idx = b * 8 + bit
          break if idx >= count
          bits[idx] = (byte & (1_u8 << bit)) != 0
        end
      end
      bits
    end
  end
end
