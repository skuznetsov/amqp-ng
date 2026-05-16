require "./connection_methods"

module Amqp::Wire::AmqpZeroNineOne
  METHOD_ID_TX_SELECT      = 10_u16
  METHOD_ID_TX_SELECT_OK   = 11_u16
  METHOD_ID_TX_COMMIT      = 20_u16
  METHOD_ID_TX_COMMIT_OK   = 21_u16
  METHOD_ID_TX_ROLLBACK    = 30_u16
  METHOD_ID_TX_ROLLBACK_OK = 31_u16

  module TxMethods
    extend self

    def payload(method_id : UInt16) : Bytes
      io = IO::Memory.new
      io.write_bytes(CLASS_ID_TX, IO::ByteFormat::NetworkEndian)
      io.write_bytes(method_id, IO::ByteFormat::NetworkEndian)
      io.to_slice
    end

    struct Select
      def to_payload : Bytes
        TxMethods.payload(METHOD_ID_TX_SELECT)
      end
    end

    struct SelectOk
      def self.read(io : IO) : self
        new
      end
    end

    struct Commit
      def to_payload : Bytes
        TxMethods.payload(METHOD_ID_TX_COMMIT)
      end
    end

    struct CommitOk
      def self.read(io : IO) : self
        new
      end
    end

    struct Rollback
      def to_payload : Bytes
        TxMethods.payload(METHOD_ID_TX_ROLLBACK)
      end
    end

    struct RollbackOk
      def self.read(io : IO) : self
        new
      end
    end
  end
end
