require "./types"
require "./bit_pack"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_CONFIRM_SELECT    = 10_u16
  METHOD_ID_CONFIRM_SELECT_OK = 11_u16

  module ConfirmMethods
    extend self

    struct Select
      getter no_wait : Bool

      def initialize(@no_wait = false)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        io.write_bytes(CLASS_ID_CONFIRM, IO::ByteFormat::NetworkEndian)
        io.write_bytes(METHOD_ID_CONFIRM_SELECT, IO::ByteFormat::NetworkEndian)
        BitPack.write(io, [@no_wait])
        io.to_slice
      end
    end

    struct SelectOk
      def self.read(io : IO) : self
        new
      end
    end
  end
end
