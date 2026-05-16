require "../../error"
require "../../arguments"
require "./types"

module Amqp::Wire::AmqpZeroNineOne
  # Class ids (see docs/05-wire-0-9-1/02-classes-methods.md §1).
  CLASS_ID_CONNECTION = 10_u16
  CLASS_ID_CHANNEL    = 20_u16
  CLASS_ID_EXCHANGE   = 40_u16
  CLASS_ID_QUEUE      = 50_u16
  CLASS_ID_BASIC      = 60_u16
  CLASS_ID_CONFIRM    = 85_u16
  CLASS_ID_TX         = 90_u16

  # Method ids for class 10 (connection).
  METHOD_ID_CONNECTION_START     = 10_u16
  METHOD_ID_CONNECTION_START_OK  = 11_u16
  METHOD_ID_CONNECTION_SECURE    = 20_u16
  METHOD_ID_CONNECTION_SECURE_OK = 21_u16
  METHOD_ID_CONNECTION_TUNE      = 30_u16
  METHOD_ID_CONNECTION_TUNE_OK   = 31_u16
  METHOD_ID_CONNECTION_OPEN      = 40_u16
  METHOD_ID_CONNECTION_OPEN_OK   = 41_u16
  METHOD_ID_CONNECTION_CLOSE     = 50_u16
  METHOD_ID_CONNECTION_CLOSE_OK  = 51_u16
  METHOD_ID_CONNECTION_BLOCKED   = 60_u16
  METHOD_ID_CONNECTION_UNBLOCKED = 61_u16

  module ConnectionMethods
    extend self

    private def write_method_header(io : IO, method_id : UInt16) : Nil
      io.write_bytes(CLASS_ID_CONNECTION, IO::ByteFormat::NetworkEndian)
      io.write_bytes(method_id, IO::ByteFormat::NetworkEndian)
    end

    # ---- connection.start (server→client) -------------------------------
    struct Start
      getter version_major : UInt8
      getter version_minor : UInt8
      getter server_properties : Amqp::Arguments
      getter mechanisms : String
      getter locales : String

      def initialize(@version_major, @version_minor, @server_properties,
                     @mechanisms, @locales)
      end

      def self.read(io : IO) : self
        major = io.read_byte || raise Amqp::ProtocolError.new("eof in connection.start version-major")
        minor = io.read_byte || raise Amqp::ProtocolError.new("eof in connection.start version-minor")
        props = Types.read_field_table(io)
        mechs = Types.read_longstr_string(io)
        locs = Types.read_longstr_string(io)
        new(major, minor, props, mechs, locs)
      end
    end

    # ---- connection.start-ok (client→server) ----------------------------
    struct StartOk
      getter client_properties : Amqp::Arguments
      getter mechanism : String
      getter response : Bytes
      getter locale : String

      def initialize(@client_properties, @mechanism, @response, @locale)
      end

      # Builds the method payload (class-id + method-id + args).
      def to_payload : Bytes
        io = IO::Memory.new
        ConnectionMethods.write_method_payload(io, METHOD_ID_CONNECTION_START_OK) do
          Types.write_field_table(io, @client_properties)
          Types.write_shortstr(io, @mechanism)
          Types.write_longstr(io, @response)
          Types.write_shortstr(io, @locale)
        end
        io.to_slice
      end
    end

    # ---- connection.tune (server→client) --------------------------------
    struct Tune
      getter channel_max : UInt16
      getter frame_max : UInt32
      getter heartbeat : UInt16

      def initialize(@channel_max, @frame_max, @heartbeat)
      end

      def self.read(io : IO) : self
        cm = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        fm = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
        hb = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        new(cm, fm, hb)
      end
    end

    # ---- connection.tune-ok (client→server) -----------------------------
    struct TuneOk
      getter channel_max : UInt16
      getter frame_max : UInt32
      getter heartbeat : UInt16

      def initialize(@channel_max, @frame_max, @heartbeat)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        ConnectionMethods.write_method_payload(io, METHOD_ID_CONNECTION_TUNE_OK) do
          io.write_bytes(@channel_max, IO::ByteFormat::NetworkEndian)
          io.write_bytes(@frame_max, IO::ByteFormat::NetworkEndian)
          io.write_bytes(@heartbeat, IO::ByteFormat::NetworkEndian)
        end
        io.to_slice
      end
    end

    # ---- connection.open (client→server) --------------------------------
    struct Open
      getter virtual_host : String

      def initialize(@virtual_host)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        ConnectionMethods.write_method_payload(io, METHOD_ID_CONNECTION_OPEN) do
          Types.write_shortstr(io, @virtual_host)
          Types.write_shortstr(io, "") # reserved-1
          io.write_byte(0_u8)          # reserved-2 (bit packed alone)
        end
        io.to_slice
      end
    end

    # ---- connection.open-ok (server→client) -----------------------------
    struct OpenOk
      getter reserved : String

      def initialize(@reserved)
      end

      def self.read(io : IO) : self
        new(Types.read_shortstr(io))
      end
    end

    # ---- connection.close (both directions) -----------------------------
    struct Close
      getter reply_code : UInt16
      getter reply_text : String
      getter class_id : UInt16
      getter method_id : UInt16

      def initialize(@reply_code, @reply_text, @class_id, @method_id)
      end

      def self.read(io : IO) : self
        rc = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        rt = Types.read_shortstr(io)
        cid = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        mid = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
        new(rc, rt, cid, mid)
      end

      def to_payload : Bytes
        io = IO::Memory.new
        ConnectionMethods.write_method_payload(io, METHOD_ID_CONNECTION_CLOSE) do
          io.write_bytes(@reply_code, IO::ByteFormat::NetworkEndian)
          Types.write_shortstr(io, @reply_text)
          io.write_bytes(@class_id, IO::ByteFormat::NetworkEndian)
          io.write_bytes(@method_id, IO::ByteFormat::NetworkEndian)
        end
        io.to_slice
      end
    end

    # ---- connection.close-ok (both directions) --------------------------
    struct CloseOk
      def self.read(io : IO) : self
        new
      end

      def to_payload : Bytes
        io = IO::Memory.new
        ConnectionMethods.write_method_payload(io, METHOD_ID_CONNECTION_CLOSE_OK) { }
        io.to_slice
      end
    end

    struct Blocked
      getter reason : String

      def initialize(@reason)
      end

      def self.read(io : IO) : self
        new(Types.read_shortstr(io))
      end

      def to_payload : Bytes
        io = IO::Memory.new
        ConnectionMethods.write_method_payload(io, METHOD_ID_CONNECTION_BLOCKED) do
          Types.write_shortstr(io, @reason)
        end
        io.to_slice
      end
    end

    struct Unblocked
      def self.read(io : IO) : self
        new
      end

      def to_payload : Bytes
        io = IO::Memory.new
        ConnectionMethods.write_method_payload(io, METHOD_ID_CONNECTION_UNBLOCKED) { }
        io.to_slice
      end
    end

    # Helper that writes class-id + method-id then yields for arg encoding.
    def write_method_payload(io : IO, method_id : UInt16, & : -> Nil) : Nil
      io.write_bytes(CLASS_ID_CONNECTION, IO::ByteFormat::NetworkEndian)
      io.write_bytes(method_id, IO::ByteFormat::NetworkEndian)
      yield
    end
  end
end
