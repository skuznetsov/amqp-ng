require "../../error"
require "../../properties"
require "./types"

module Amqp::Wire::AmqpZeroNineOne
  module ContentHeader
    extend self

    # Flag-bit constants (MSB→LSB per the spec table).
    FLAG_CONTENT_TYPE     = 0x8000_u16
    FLAG_CONTENT_ENCODING = 0x4000_u16
    FLAG_HEADERS          = 0x2000_u16
    FLAG_DELIVERY_MODE    = 0x1000_u16
    FLAG_PRIORITY         = 0x0800_u16
    FLAG_CORRELATION_ID   = 0x0400_u16
    FLAG_REPLY_TO         = 0x0200_u16
    FLAG_EXPIRATION       = 0x0100_u16
    FLAG_MESSAGE_ID       = 0x0080_u16
    FLAG_TIMESTAMP        = 0x0040_u16
    FLAG_TYPE             = 0x0020_u16
    FLAG_USER_ID          = 0x0010_u16
    FLAG_APP_ID           = 0x0008_u16
    FLAG_CLUSTER_ID       = 0x0004_u16

    # Builds the full header-frame payload (class-id .. property-list).
    def encode(class_id : UInt16, body_size : UInt64, props : Amqp::Properties) : Bytes
      io = IO::Memory.new
      io.write_bytes(class_id, IO::ByteFormat::NetworkEndian)
      io.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
      io.write_bytes(body_size, IO::ByteFormat::NetworkEndian)

      flags = 0_u16
      body = IO::Memory.new

      if ct = props.content_type
        flags |= FLAG_CONTENT_TYPE
        Types.write_shortstr(body, ct)
      end
      if ce = props.content_encoding
        flags |= FLAG_CONTENT_ENCODING
        Types.write_shortstr(body, ce)
      end
      if h = props.headers
        flags |= FLAG_HEADERS
        Types.write_field_table(body, h)
      end
      if dm = props.delivery_mode
        flags |= FLAG_DELIVERY_MODE
        body.write_byte(dm.value)
      end
      if pr = props.priority
        flags |= FLAG_PRIORITY
        body.write_byte(pr)
      end
      if cid = props.correlation_id
        flags |= FLAG_CORRELATION_ID
        Types.write_shortstr(body, cid)
      end
      if rt = props.reply_to
        flags |= FLAG_REPLY_TO
        Types.write_shortstr(body, rt)
      end
      if ex = props.expiration
        unless ex.each_char.all?(&.ascii_number?)
          raise Amqp::ConfigurationError.new("expiration must be ASCII decimal: #{ex.inspect}")
        end
        flags |= FLAG_EXPIRATION
        Types.write_shortstr(body, ex)
      end
      if mid = props.message_id
        flags |= FLAG_MESSAGE_ID
        Types.write_shortstr(body, mid)
      end
      if ts = props.timestamp
        flags |= FLAG_TIMESTAMP
        body.write_bytes(ts.to_unix.to_i64, IO::ByteFormat::NetworkEndian)
      end
      if t = props.type
        flags |= FLAG_TYPE
        Types.write_shortstr(body, t)
      end
      if uid = props.user_id
        flags |= FLAG_USER_ID
        Types.write_shortstr(body, uid)
      end
      if aid = props.app_id
        flags |= FLAG_APP_ID
        Types.write_shortstr(body, aid)
      end
      if clid = props.cluster_id
        flags |= FLAG_CLUSTER_ID
        Types.write_shortstr(body, clid)
      end

      io.write_bytes(flags, IO::ByteFormat::NetworkEndian)
      io.write(body.to_slice) if body.bytesize > 0
      io.to_slice
    end

    record Decoded, class_id : UInt16, body_size : UInt64, properties : Amqp::Properties

    def decode(payload : Bytes) : Decoded
      io = IO::Memory.new(payload, false)
      class_id = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
      weight = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
      raise Amqp::ProtocolError.new("content-header weight #{weight} != 0") if weight != 0
      body_size = io.read_bytes(UInt64, IO::ByteFormat::NetworkEndian)
      flags = io.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
      raise Amqp::ProtocolError.new("content-header continuation bit set") if (flags & 0x0001) != 0

      props = Amqp::Properties.new
      props.content_type     = Types.read_shortstr(io) if (flags & FLAG_CONTENT_TYPE) != 0
      props.content_encoding = Types.read_shortstr(io) if (flags & FLAG_CONTENT_ENCODING) != 0
      props.headers          = Types.read_field_table(io) if (flags & FLAG_HEADERS) != 0
      if (flags & FLAG_DELIVERY_MODE) != 0
        raw = io.read_byte || raise Amqp::ProtocolError.new("eof reading delivery-mode")
        props.delivery_mode = case raw
                              when 1_u8 then Amqp::Properties::Persistence::Transient
                              when 2_u8 then Amqp::Properties::Persistence::Persistent
                              else
                                raise Amqp::ProtocolError.new("invalid delivery-mode #{raw}")
                              end
      end
      if (flags & FLAG_PRIORITY) != 0
        props.priority = io.read_byte || raise Amqp::ProtocolError.new("eof reading priority")
      end
      props.correlation_id = Types.read_shortstr(io) if (flags & FLAG_CORRELATION_ID) != 0
      props.reply_to       = Types.read_shortstr(io) if (flags & FLAG_REPLY_TO) != 0
      props.expiration     = Types.read_shortstr(io) if (flags & FLAG_EXPIRATION) != 0
      props.message_id     = Types.read_shortstr(io) if (flags & FLAG_MESSAGE_ID) != 0
      if (flags & FLAG_TIMESTAMP) != 0
        secs = io.read_bytes(Int64, IO::ByteFormat::NetworkEndian)
        props.timestamp = Time.unix(secs)
      end
      props.type       = Types.read_shortstr(io) if (flags & FLAG_TYPE) != 0
      props.user_id    = Types.read_shortstr(io) if (flags & FLAG_USER_ID) != 0
      props.app_id     = Types.read_shortstr(io) if (flags & FLAG_APP_ID) != 0
      props.cluster_id = Types.read_shortstr(io) if (flags & FLAG_CLUSTER_ID) != 0

      Decoded.new(class_id, body_size, props)
    end
  end
end
