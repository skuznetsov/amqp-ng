require "log"
require "./error"
require "./arguments"
require "./properties"
require "./delivery"
require "./subscription"
require "./queue_info"
require "./wire/frame"
require "./wire/amqp_zero_nine_one/channel_methods"
require "./wire/amqp_zero_nine_one/exchange_methods"
require "./wire/amqp_zero_nine_one/queue_methods"
require "./wire/amqp_zero_nine_one/basic_methods"
require "./wire/amqp_zero_nine_one/confirm_methods"
require "./wire/amqp_zero_nine_one/content_header"

module Amqp
  class Channel
    Log = ::Log.for("amqp.channel")
    enum State
      Initial
      Open
      Recovering
      Closing
      Closed
    end

    # Envelope passed to a waiting sync RPC.
    private struct MethodEnvelope
      getter class_id : UInt16
      getter method_id : UInt16
      getter body : IO::Memory

      def initialize(@class_id, @method_id, @body)
      end
    end

    # Topology entries — recorded after a successful sync_rpc so that
    # Connection recovery can replay every declaration / binding /
    # consumer on the new socket session.

    private record ExchangeOp,
      name : String,
      type : String,
      passive : Bool,
      durable : Bool,
      auto_delete : Bool,
      internal : Bool,
      arguments : Amqp::Arguments

    private record QueueOp,
      declared_name : String,
      actual_name : String,
      passive : Bool,
      durable : Bool,
      exclusive : Bool,
      auto_delete : Bool,
      arguments : Amqp::Arguments

    private record BindOp,
      queue : String,
      exchange : String,
      routing_key : String,
      arguments : Amqp::Arguments

    private record QosOp,
      prefetch_count : UInt16,
      global : Bool,
      prefetch_size : UInt32

    private record ConsumeOp,
      queue : String,
      consumer_tag : String,
      no_local : Bool,
      no_ack : Bool,
      exclusive : Bool,
      arguments : Amqp::Arguments

    getter id : UInt16
    @connection : Connection
    @state : State
    @inbox : ::Channel(Amqp::Wire::Frame)
    @sync_mutex : Mutex
    @sync_slot : ::Channel(MethodEnvelope | Exception)?
    @consumers : Hash(String, Subscription)
    @consumers_mutex : Mutex
    @pending_method : (Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver |
                       Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return |
                       Nil)
    @pending_props : Properties?
    @pending_body_size : UInt64
    @pending_body_received : UInt64
    @pending_body : IO::Memory
    @close_reason : Exception?
    @handler_done : ::Channel(Nil)
    @confirms_enabled : Bool
    @confirms_mutex : Mutex
    @next_publish_seq : UInt64
    @unconfirmed : Set(UInt64)
    @confirms_nacked : Bool
    @confirms_waker : ::Channel(Nil)
    @topology_mutex : Mutex
    @topology_exchanges : Hash(String, ExchangeOp)
    @topology_queues : Hash(String, QueueOp)
    @topology_bindings : Array(BindOp)
    @topology_qos : QosOp?
    @topology_consumers : Hash(String, ConsumeOp)

    protected def initialize(@connection : Connection, @id : UInt16)
      @state = State::Initial
      @inbox = ::Channel(Amqp::Wire::Frame).new(256)
      @sync_mutex = Mutex.new
      @sync_slot = nil
      @consumers = {} of String => Subscription
      @consumers_mutex = Mutex.new
      @pending_method = nil
      @pending_props = nil
      @pending_body_size = 0_u64
      @pending_body_received = 0_u64
      @pending_body = IO::Memory.new
      @close_reason = nil
      @handler_done = ::Channel(Nil).new
      @confirms_enabled = false
      @confirms_mutex = Mutex.new
      @next_publish_seq = 1_u64
      @unconfirmed = Set(UInt64).new
      @confirms_nacked = false
      @confirms_waker = ::Channel(Nil).new
      @topology_mutex = Mutex.new
      @topology_exchanges = {} of String => ExchangeOp
      @topology_queues = {} of String => QueueOp
      @topology_bindings = [] of BindOp
      @topology_qos = nil
      @topology_consumers = {} of String => ConsumeOp
    end

    protected def open : Nil
      spawn(name: "amqp-channel-#{@id}") { run_handler }
      env = sync_rpc(Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Open.new.to_payload)
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CHANNEL,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CHANNEL_OPEN_OK)
      Amqp::Wire::AmqpZeroNineOne::ChannelMethods::OpenOk.read(env.body)
      @state = State::Open
    end

    def closed? : Bool
      @state == State::Closed
    end

    def close(reply_code : UInt16 = 200_u16, reply_text : String = "OK") : Nil
      return if @state == State::Closed
      @state = State::Closing
      begin
        env = sync_rpc(
          Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Close.new(
            reply_code, reply_text, 0_u16, 0_u16,
          ).to_payload
        )
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CHANNEL,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CHANNEL_CLOSE_OK)
      rescue
        # The handler fiber may have already aborted us with the same close.
      end
      finalize_closed(ChannelClosedByCaller.new("channel #{@id} closed by caller"))
    end

    # ---- Frame intake (called from connection reader fiber) -------------

    protected def dispatch_frame(frame : Amqp::Wire::Frame) : Nil
      @inbox.send(frame)
    rescue ::Channel::ClosedError
      # Channel already torn down; drop frame.
    end

    protected def abort_with(reason : Exception) : Nil
      return if @state == State::Closed
      @close_reason = reason
      finalize_closed(reason)
    end

    # ---- Public ops -----------------------------------------------------

    def exchange_declare(name : String,
                         type : String = "direct",
                         passive : Bool = false,
                         durable : Bool = false,
                         auto_delete : Bool = false,
                         internal : Bool = false,
                         arguments : Amqp::Arguments = Amqp::Arguments.new) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::ExchangeMethods::Declare.new(
          name, type, passive, durable, auto_delete, internal, arguments,
        ).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_EXCHANGE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_EXCHANGE_DECLARE_OK)
      unless name.empty? || passive
        @topology_mutex.synchronize do
          @topology_exchanges[name] = ExchangeOp.new(
            name, type, passive, durable, auto_delete, internal, arguments,
          )
        end
      end
    end

    def queue_declare(name : String = "",
                      passive : Bool = false,
                      durable : Bool = false,
                      exclusive : Bool = false,
                      auto_delete : Bool = false,
                      arguments : Amqp::Arguments = Amqp::Arguments.new) : QueueInfo
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::QueueMethods::Declare.new(
          name, passive, durable, exclusive, auto_delete, arguments,
        ).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_DECLARE_OK)
      ok = Amqp::Wire::AmqpZeroNineOne::QueueMethods::DeclareOk.read(env.body)
      unless passive
        @topology_mutex.synchronize do
          @topology_queues[ok.name] = QueueOp.new(
            name, ok.name, passive, durable, exclusive, auto_delete, arguments,
          )
        end
      end
      QueueInfo.new(ok.name, ok.message_count, ok.consumer_count)
    end

    def queue_bind(queue : String,
                   exchange : String,
                   routing_key : String = "",
                   arguments : Amqp::Arguments = Amqp::Arguments.new) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::QueueMethods::Bind.new(
          queue, exchange, routing_key, arguments,
        ).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_BIND_OK)
      @topology_mutex.synchronize do
        @topology_bindings << BindOp.new(queue, exchange, routing_key, arguments)
      end
    end

    def queue_delete(name : String, if_unused : Bool = false, if_empty : Bool = false) : UInt32
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::QueueMethods::Delete.new(name, if_unused, if_empty).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_DELETE_OK)
      @topology_mutex.synchronize do
        @topology_queues.delete(name)
        @topology_bindings.reject! { |b| b.queue == name }
      end
      Amqp::Wire::AmqpZeroNineOne::QueueMethods::DeleteOk.read(env.body).message_count
    end

    def qos(prefetch_count : UInt16, global : Bool = false, prefetch_size : UInt32 = 0_u32) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::BasicMethods::Qos.new(prefetch_size, prefetch_count, global).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_QOS_OK)
      @topology_mutex.synchronize do
        @topology_qos = QosOp.new(prefetch_count, global, prefetch_size)
      end
    end

    def publish(exchange : String,
                routing_key : String,
                body : Bytes,
                properties : Properties = Properties.new,
                mandatory : Bool = false) : UInt64?
      ensure_open!
      method_payload = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Publish.new(
        exchange, routing_key, mandatory,
      ).to_payload
      header_payload = Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
        Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
        body.size.to_u64,
        properties,
      )
      frame_max = @connection.frame_max
      max_body = max_body_per_frame(frame_max)

      # In confirm mode we must register the seq BEFORE the broker can
      # possibly ack it. Hold @confirms_mutex across the write so any
      # incoming basic.ack on the handler fiber finds the seq in
      # @unconfirmed and doesn't race past us.
      seq = nil
      if @confirms_enabled
        @confirms_mutex.lock
      end
      begin
        if @confirms_enabled
          seq = @next_publish_seq
          @unconfirmed << seq
          @next_publish_seq += 1
        end

        @connection.with_write do |io|
          Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Method, @id, method_payload).write(io)
          Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Header, @id, header_payload).write(io)
          offset = 0
          while offset < body.size
            chunk = Math.min(max_body, body.size - offset)
            slice = body[offset, chunk]
            Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Body, @id, slice).write(io)
            offset += chunk
          end
        end
      ensure
        @confirms_mutex.unlock if @confirms_enabled
      end
      @connection.stats.incr_published
      seq
    end

    def confirm_select : Nil
      return if @confirms_enabled
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::ConfirmMethods::Select.new(no_wait: false).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CONFIRM,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONFIRM_SELECT_OK)
      Amqp::Wire::AmqpZeroNineOne::ConfirmMethods::SelectOk.read(env.body)
      @confirms_mutex.synchronize do
        @confirms_enabled = true
        @next_publish_seq = 1_u64
        @unconfirmed.clear
        @confirms_nacked = false
      end
    end

    def confirms? : Bool
      @confirms_enabled
    end

    # Block until every publish issued so far in this channel has been
    # settled by the broker. Returns true if all were acked, false if
    # any was nacked or the timeout expired. Raises if the channel is
    # closed while waiting.
    def wait_for_confirms(timeout : Time::Span = 60.seconds) : Bool
      raise ConcurrencyError.new("channel #{@id} is not in confirm mode") unless @confirms_enabled

      target_seq = @confirms_mutex.synchronize { @next_publish_seq - 1_u64 }
      return !@confirms_nacked if target_seq == 0

      deadline = Time.monotonic + timeout
      loop do
        settled, nacked, waker = @confirms_mutex.synchronize do
          done = @unconfirmed.empty? || @unconfirmed.min > target_seq
          {done, @confirms_nacked, @confirms_waker}
        end
        return !nacked if settled
        if @state == State::Closed
          raise (@close_reason || ChannelClosedByCaller.new("channel #{@id} closed"))
        end
        remaining = deadline - Time.monotonic
        return false if remaining <= Time::Span.zero
        select
        when waker.receive?
          # signaled — re-check on next iteration
        when timeout(remaining)
          return false
        end
      end
    end

    private def max_body_per_frame(frame_max : UInt32) : Int32
      effective = frame_max == 0 ? 131_072_u32 : frame_max
      # Reserve 8 bytes for frame envelope (1+2+4+1).
      (effective.to_i32 - 8).clamp(1, Int32::MAX)
    end

    def consume(queue : String,
                consumer_tag : String = "",
                no_local : Bool = false,
                no_ack : Bool = false,
                exclusive : Bool = false,
                arguments : Amqp::Arguments = Amqp::Arguments.new) : Subscription
      # Pre-generate a client-side tag if caller didn't supply one. This
      # lets us register the Subscription BEFORE the broker can send any
      # basic.deliver — otherwise a fast broker (or anything that races
      # the handler fiber) could route a delivery against an unknown tag.
      tag = consumer_tag.empty? ? "amqp-ng-ctag-#{Random::Secure.hex(8)}" : consumer_tag
      sub = Subscription.new(self, tag)
      @consumers_mutex.synchronize { @consumers[tag] = sub }

      begin
        env = sync_rpc(
          Amqp::Wire::AmqpZeroNineOne::BasicMethods::Consume.new(
            queue, tag, no_local, no_ack, exclusive, arguments,
          ).to_payload
        )
      rescue ex
        @consumers_mutex.synchronize { @consumers.delete(tag) }
        sub.mark_closed
        raise ex
      end

      begin
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CONSUME_OK)
        Amqp::Wire::AmqpZeroNineOne::BasicMethods::ConsumeOk.read(env.body)
      rescue ex
        @consumers_mutex.synchronize { @consumers.delete(tag) }
        sub.mark_closed
        raise ex
      end

      @topology_mutex.synchronize do
        @topology_consumers[tag] = ConsumeOp.new(
          queue, tag, no_local, no_ack, exclusive, arguments,
        )
      end
      sub
    end

    def cancel(consumer_tag : String) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::BasicMethods::Cancel.new(consumer_tag).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CANCEL_OK)
      sub = @consumers_mutex.synchronize { @consumers.delete(consumer_tag) }
      @topology_mutex.synchronize { @topology_consumers.delete(consumer_tag) }
      sub.try &.mark_closed
    end

    def ack(delivery_tag : UInt64, multiple : Bool = false) : Nil
      ensure_open!
      payload = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Ack.new(delivery_tag, multiple).to_payload
      @connection.write_frame(@id, Amqp::Wire::FrameType::Method, payload)
    end

    def nack(delivery_tag : UInt64, multiple : Bool = false, requeue : Bool = true) : Nil
      ensure_open!
      payload = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Nack.new(delivery_tag, multiple, requeue).to_payload
      @connection.write_frame(@id, Amqp::Wire::FrameType::Method, payload)
    end

    def reject(delivery_tag : UInt64, requeue : Bool = true) : Nil
      ensure_open!
      payload = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Reject.new(delivery_tag, requeue).to_payload
      @connection.write_frame(@id, Amqp::Wire::FrameType::Method, payload)
    end

    # ---- Internals -----------------------------------------------------

    private def ensure_open! : Nil
      case @state
      when .open?
        # ok
      when .recovering?
        raise RecoveryInProgress.new("channel #{@id} is recovering")
      when .closed?
        raise ChannelClosedByCaller.new("channel #{@id} is closed")
      else
        raise ConcurrencyError.new("channel #{@id} state #{@state}")
      end
    end

    private def sync_rpc(payload : Bytes) : MethodEnvelope
      @sync_mutex.synchronize do
        case @state
        when .recovering?
          raise RecoveryInProgress.new("channel #{@id} is recovering")
        when .closed?
          raise (@close_reason || ChannelClosedByCaller.new("channel #{@id} closed"))
        end
        slot = ::Channel(MethodEnvelope | Exception).new(1)
        @sync_slot = slot
        begin
          @connection.write_frame(@id, Amqp::Wire::FrameType::Method, payload)
        rescue ex
          @sync_slot = nil
          raise ex
        end

        reply = slot.receive
        @sync_slot = nil
        case reply
        in Exception
          raise reply
        in MethodEnvelope
          reply
        end
      end
    end

    # Internal version that bypasses the Recovering gate. Used by the
    # recovery driver itself to replay topology after a successful
    # handshake but before flipping state back to Open.
    private def sync_rpc_unchecked(payload : Bytes) : MethodEnvelope
      @sync_mutex.synchronize do
        slot = ::Channel(MethodEnvelope | Exception).new(1)
        @sync_slot = slot
        begin
          @connection.write_frame(@id, Amqp::Wire::FrameType::Method, payload)
        rescue ex
          @sync_slot = nil
          raise ex
        end

        reply = slot.receive
        @sync_slot = nil
        case reply
        in Exception
          raise reply
        in MethodEnvelope
          reply
        end
      end
    end

    private def expect_method!(env : MethodEnvelope,
                               class_id : UInt16,
                               method_id : UInt16) : Nil
      return if env.class_id == class_id && env.method_id == method_id
      raise ProtocolError.new(
        "channel #{@id}: expected (#{class_id},#{method_id}), got (#{env.class_id},#{env.method_id})"
      )
    end

    # ---- Handler fiber --------------------------------------------------

    private def run_handler : Nil
      loop do
        frame = @inbox.receive?
        break if frame.nil?
        process_frame(frame)
      end
    rescue ex : Amqp::Error
      finalize_closed(ex)
    rescue ex : Exception
      finalize_closed(ProtocolError.new("channel handler error: #{ex.message}", ex))
    ensure
      @handler_done.close rescue nil
    end

    private def process_frame(frame : Amqp::Wire::Frame) : Nil
      case frame.type
      when .method?
        process_method_frame(frame)
      when .header?
        process_header_frame(frame)
      when .body?
        process_body_frame(frame)
      else
        raise ProtocolError.new("unexpected frame type #{frame.type} on channel #{@id}")
      end
    end

    private def process_method_frame(frame : Amqp::Wire::Frame) : Nil
      if @pending_method
        raise ProtocolError.new("channel #{@id}: method frame mid-content")
      end
      body = IO::Memory.new(frame.payload, false)
      class_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)
      method_id = body.read_bytes(UInt16, IO::ByteFormat::NetworkEndian)

      # Broker-initiated channel.close
      if class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CHANNEL &&
         method_id == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CHANNEL_CLOSE
        cls = Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Close.read(body)
        begin
          @connection.write_frame(@id, Amqp::Wire::FrameType::Method,
            Amqp::Wire::AmqpZeroNineOne::ChannelMethods::CloseOk.new.to_payload)
        rescue
        end
        exc = map_channel_close(cls)
        notify_sync_failure(exc)
        finalize_closed(exc)
        return
      end

      # Content-bearing methods → enter assembly mode.
      if class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC
        case method_id
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_DELIVER
          @pending_method = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver.read(body)
          @pending_body = IO::Memory.new
          @pending_body_received = 0_u64
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_RETURN
          @pending_method = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return.read(body)
          @pending_body = IO::Memory.new
          @pending_body_received = 0_u64
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CANCEL
          cancel = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Cancel.read(body)
          sub = @consumers_mutex.synchronize { @consumers.delete(cancel.consumer_tag) }
          @topology_mutex.synchronize { @topology_consumers.delete(cancel.consumer_tag) }
          sub.try &.mark_closed
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_ACK
          ack = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Ack.read(body)
          settle_publish(ack.delivery_tag, ack.multiple, nacked: false)
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_NACK
          nack = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Nack.read(body)
          settle_publish(nack.delivery_tag, nack.multiple, nacked: true)
          return
        end
      end

      # Otherwise: synchronous reply to whatever the caller is awaiting.
      # `body` is already positioned right after class+method ids.
      slot = @sync_slot
      if slot
        slot.send(MethodEnvelope.new(class_id, method_id, body))
      else
        raise ProtocolError.new("channel #{@id}: unsolicited method (#{class_id},#{method_id})")
      end
    end

    private def process_header_frame(frame : Amqp::Wire::Frame) : Nil
      pending = @pending_method
      raise ProtocolError.new("channel #{@id}: header without method") if pending.nil?
      decoded = Amqp::Wire::AmqpZeroNineOne::ContentHeader.decode(frame.payload)
      unless decoded.class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC
        raise ProtocolError.new("channel #{@id}: header class #{decoded.class_id} != 60")
      end
      @pending_props = decoded.properties
      @pending_body_size = decoded.body_size
      @pending_body_received = 0_u64
      if @pending_body_size == 0
        emit_pending_delivery
      end
    end

    private def process_body_frame(frame : Amqp::Wire::Frame) : Nil
      raise ProtocolError.new("channel #{@id}: body without header") if @pending_method.nil?
      @pending_body.write(frame.payload)
      @pending_body_received += frame.payload.size.to_u64
      if @pending_body_received > @pending_body_size
        raise ProtocolError.new("channel #{@id}: body fragment overflows declared size")
      end
      if @pending_body_received == @pending_body_size
        emit_pending_delivery
      end
    end

    private def emit_pending_delivery : Nil
      method = @pending_method
      props = @pending_props || Properties.new
      body_bytes = @pending_body.to_slice
      @pending_method = nil
      @pending_props = nil
      @pending_body_size = 0_u64
      @pending_body_received = 0_u64
      @pending_body = IO::Memory.new

      case method
      in Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver
        delivery = Delivery.new(
          consumer_tag: method.consumer_tag,
          delivery_tag: method.delivery_tag,
          redelivered: method.redelivered,
          exchange: method.exchange,
          routing_key: method.routing_key,
          properties: props,
          body: body_bytes,
          channel: self,
        )
        sub = @consumers_mutex.synchronize { @consumers[method.consumer_tag]? }
        sub.try &.deliver(delivery)
        @connection.stats.incr_consumed
      in Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return
        # Mandatory-publish unroutable messages: stats only for now;
        # caller-visible return handling lives in a later slice.
        @connection.stats.incr_returned
      in Nil
        # nothing
      end
    end

    private def map_channel_close(close : Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Close) : Exception
      case close.reply_code
      when 406_u16
        PreconditionFailedError.new(close.reply_code, close.reply_text)
      else
        ChannelClosedByBroker.new(close.reply_code, close.reply_text,
          close.class_id, close.method_id)
      end
    end

    private def notify_sync_failure(exc : Exception) : Nil
      slot = @sync_slot
      slot.try &.send(exc)
    end

    # Settle a single seq (multiple=false) or all seqs up to and including
    # `tag` (multiple=true). Sets nack-flag if any settled seq was negative,
    # then wakes any wait_for_confirms waiters.
    private def settle_publish(tag : UInt64, multiple : Bool, nacked : Bool) : Nil
      settled = 0
      @confirms_mutex.synchronize do
        if multiple
          before = @unconfirmed.size
          @unconfirmed.reject! { |s| s <= tag }
          settled = before - @unconfirmed.size
        else
          settled = @unconfirmed.delete(tag) ? 1 : 0
        end
        @confirms_nacked = true if nacked
        wake_confirms_locked
      end
      if settled > 0
        delta = settled.to_i64
        if nacked
          @connection.stats.incr_confirmed_nack(delta)
        else
          @connection.stats.incr_confirmed_ack(delta)
        end
      end
    end

    # Caller MUST already hold @confirms_mutex.
    private def wake_confirms_locked : Nil
      old = @confirms_waker
      @confirms_waker = ::Channel(Nil).new
      old.close rescue nil
    end

    private def finalize_closed(reason : Exception) : Nil
      return if @state == State::Closed
      @state = State::Closed
      @close_reason ||= reason
      notify_sync_failure(reason)
      @consumers_mutex.synchronize do
        @consumers.each_value(&.mark_closed)
        @consumers.clear
      end
      @confirms_mutex.synchronize { wake_confirms_locked }
      @connection.unregister_channel(@id)
      @inbox.close rescue nil
    end

    # ---- Recovery hooks (called from Connection on socket loss) ---------

    # Suspend the channel for recovery. Any in-flight sync_rpc fails with
    # RecoveryInProgress, content-assembly state resets, subscriptions
    # drop buffered (now stale) deliveries, outstanding publish confirms
    # are marked nacked, and the handler fiber exits cleanly.
    protected def enter_recovery(reason : Exception) : Nil
      return if @state == State::Closed
      @state = State::Recovering
      notify_sync_failure(RecoveryInProgress.new(
        "channel #{@id} recovery triggered: #{reason.message}"
      ))
      @pending_method = nil
      @pending_props = nil
      @pending_body_size = 0_u64
      @pending_body_received = 0_u64
      @pending_body = IO::Memory.new

      @consumers_mutex.synchronize do
        @consumers.each_value(&.reset_mailbox)
      end

      # Surface unconfirmed publishes as nacked so wait_for_confirms wakes.
      @confirms_mutex.synchronize do
        unless @unconfirmed.empty?
          @confirms_nacked = true
          @unconfirmed.clear
        end
        @next_publish_seq = 1_u64
        wake_confirms_locked
      end

      # Drain the inbox so any stale frames from the old reader can't
      # poison the next session, then close it — the handler exits and
      # replay_topology will spin up a fresh handler with a fresh inbox.
      @inbox.close rescue nil
    end

    # Re-open this channel against the new socket session, then replay
    # every recorded declaration / binding / qos / confirm-mode setting
    # / consumer in the same order the user originally applied them.
    # Called by the Connection recovery driver after a successful
    # handshake. On any failure, raises so the driver can surface it.
    protected def replay_topology : Nil
      return if @state == State::Closed
      @inbox = ::Channel(Amqp::Wire::Frame).new(256)
      @handler_done = ::Channel(Nil).new
      spawn(name: "amqp-channel-#{@id}") { run_handler }

      env = sync_rpc_unchecked(
        Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Open.new.to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CHANNEL,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CHANNEL_OPEN_OK)

      if @confirms_enabled
        env = sync_rpc_unchecked(
          Amqp::Wire::AmqpZeroNineOne::ConfirmMethods::Select.new(no_wait: false).to_payload
        )
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CONFIRM,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CONFIRM_SELECT_OK)
      end

      qos = @topology_mutex.synchronize { @topology_qos }
      if qos
        env = sync_rpc_unchecked(
          Amqp::Wire::AmqpZeroNineOne::BasicMethods::Qos.new(
            qos.prefetch_size, qos.prefetch_count, qos.global,
          ).to_payload
        )
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_QOS_OK)
      end

      exchanges = @topology_mutex.synchronize { @topology_exchanges.values }
      exchanges.each do |op|
        env = sync_rpc_unchecked(
          Amqp::Wire::AmqpZeroNineOne::ExchangeMethods::Declare.new(
            op.name, op.type, op.passive, op.durable, op.auto_delete,
            op.internal, op.arguments,
          ).to_payload
        )
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_EXCHANGE,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_EXCHANGE_DECLARE_OK)
      end

      queues = @topology_mutex.synchronize { @topology_queues.values.dup }
      # Maps old actual_name -> new actual_name for server-named queues
      # that get a fresh name on redeclare. Used to remap bindings,
      # consumers, and the topology_queues key.
      renames = {} of String => String
      queues.each do |op|
        # User-named queues replay under their actual_name. Server-named
        # queues (declared_name == "") must redeclare under empty name
        # because `amq.*` is a reserved prefix on the broker; we then
        # remap bindings/consumers that reference the old name.
        wire_name = op.declared_name.empty? ? "" : op.actual_name
        env = sync_rpc_unchecked(
          Amqp::Wire::AmqpZeroNineOne::QueueMethods::Declare.new(
            wire_name, op.passive, op.durable, op.exclusive,
            op.auto_delete, op.arguments,
          ).to_payload
        )
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_DECLARE_OK)
        if op.declared_name.empty?
          ok = Amqp::Wire::AmqpZeroNineOne::QueueMethods::DeclareOk.read(env.body)
          if ok.name != op.actual_name
            renames[op.actual_name] = ok.name
          end
        end
      end

      unless renames.empty?
        @topology_mutex.synchronize do
          renames.each do |old_name, new_name|
            if q = @topology_queues.delete(old_name)
              @topology_queues[new_name] = QueueOp.new(
                q.declared_name, new_name, q.passive, q.durable,
                q.exclusive, q.auto_delete, q.arguments,
              )
            end
          end
          @topology_bindings = @topology_bindings.map do |b|
            new_q = renames[b.queue]?
            new_q ? BindOp.new(new_q, b.exchange, b.routing_key, b.arguments) : b
          end
          @topology_consumers.each do |tag, c|
            if new_q = renames[c.queue]?
              @topology_consumers[tag] = ConsumeOp.new(
                new_q, c.consumer_tag, c.no_local, c.no_ack,
                c.exclusive, c.arguments,
              )
            end
          end
        end
      end

      bindings = @topology_mutex.synchronize { @topology_bindings.dup }
      bindings.each do |op|
        env = sync_rpc_unchecked(
          Amqp::Wire::AmqpZeroNineOne::QueueMethods::Bind.new(
            op.queue, op.exchange, op.routing_key, op.arguments,
          ).to_payload
        )
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_BIND_OK)
      end

      consumers = @topology_mutex.synchronize { @topology_consumers.values.dup }
      consumers.each do |op|
        env = sync_rpc_unchecked(
          Amqp::Wire::AmqpZeroNineOne::BasicMethods::Consume.new(
            op.queue, op.consumer_tag, op.no_local, op.no_ack,
            op.exclusive, op.arguments,
          ).to_payload
        )
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CONSUME_OK)
      end

      @state = State::Open
    end
  end
end
