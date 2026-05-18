require "log"
require "./error"
require "./arguments"
require "./properties"
require "./message"
require "./delivery"
require "./get_message"
require "./subscription"
require "./queue_info"
require "./wire/frame"
require "./wire/amqp_zero_nine_one/channel_methods"
require "./wire/amqp_zero_nine_one/exchange_methods"
require "./wire/amqp_zero_nine_one/queue_methods"
require "./wire/amqp_zero_nine_one/basic_methods"
require "./wire/amqp_zero_nine_one/confirm_methods"
require "./wire/amqp_zero_nine_one/tx_methods"
require "./wire/amqp_zero_nine_one/content_header"

module Amqp
  class Channel
    Log              = ::Log.for("amqp.channel")
    EMPTY_PROPERTIES = Properties.new

    alias CancelCallback = String -> Nil
    alias CloseCallback = UInt16, String -> Nil
    alias ConfirmCallback = Bool -> Nil

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

    private record PendingConfirm,
      original_tag : UInt64,
      mandatory : Bool,
      outcome : ::Channel(ConfirmOutcome)?,
      callback : ConfirmCallback?,
      replay_message : Message?,
      exchange : String,
      routing_key : String,
      sync_waiter : Bool do
      def message_for_replay : Message
        @replay_message || raise RecoveryExhaustedError.new(
          "pending confirm #{original_tag} has no replay payload"
        )
      end
    end

    private record SettledPublish,
      outcome : ConfirmOutcome,
      callback : ConfirmCallback?

    getter id : UInt16
    @connection : Connection
    @state : State
    @inbox : ::Channel(Amqp::Wire::Frame)
    @sync_mutex : Mutex
    @operation_mutex : Mutex
    @operation_busy : Bool
    @flow_mutex : Mutex
    @flow_active : Bool
    @flow_waker : ::Channel(Nil)
    @tx_enabled : Bool
    @sync_slot : ::Channel(MethodEnvelope | Exception)?
    @get_slot : ::Channel(GetMessage | Nil | Exception)?
    @consumers : Hash(String, Subscription)
    @consumers_mutex : Mutex
    @consumers_generation : Atomic(Int64)
    @cached_consumer_tag : String?
    @cached_consumer : Subscription?
    @cached_consumer_generation : Int64
    @cached_deliver_consumer_tag : String?
    @cached_deliver_exchange : String?
    @cached_deliver_routing_key : String?
    @pending_method : (Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver |
                       Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return |
                       Amqp::Wire::AmqpZeroNineOne::BasicMethods::GetOk |
                       Nil)
    @pending_props : Properties?
    @pending_body_size : UInt64
    @pending_body_received : UInt64
    @pending_body : IO::Memory
    @pending_body_direct : Bytes?
    @close_reason : Exception?
    @handler_done : ::Channel(Nil)
    @confirms_enabled : Bool
    @confirms_mutex : Mutex
    @next_publish_seq : UInt64
    @unconfirmed : Set(UInt64)
    @lowest_unconfirmed : UInt64?
    @pending_confirms : Hash(UInt64, PendingConfirm)
    @returned_confirms : Hash(UInt64, ReturnReason)
    @completed_sync_confirm_tag : UInt64?
    @completed_sync_confirm_outcome : ConfirmOutcome?
    @completed_sync_confirms : Hash(UInt64, ConfirmOutcome)
    @confirms_nacked : Bool
    @confirms_waker : ::Channel(Nil)
    @confirm_callback_queue : ::Channel(Tuple(ConfirmCallback, Bool))
    @on_return : Proc(ReturnedMessage, Nil)?
    @on_cancel : CancelCallback?
    @on_close : CloseCallback?
    @topology_mutex : Mutex
    @topology_exchanges : Hash(String, ExchangeOp)
    @topology_queues : Hash(String, QueueOp)
    @topology_bindings : Array(BindOp)
    @topology_qos : QosOp?
    @topology_consumers : Hash(String, ConsumeOp)
    @publish_frame_candidate_exchange : String?
    @publish_frame_candidate_routing_key : String?
    @publish_frame_candidate_mandatory : Bool
    @publish_frame_candidate_immediate : Bool
    @cached_publish_method_frame : Bytes?
    @cached_publish_method_frame_exchange : String?
    @cached_publish_method_frame_routing_key : String?
    @cached_publish_method_frame_mandatory : Bool
    @cached_publish_method_frame_immediate : Bool
    @empty_header_candidate_body_size : UInt64?
    @cached_empty_content_header_frame : Bytes?
    @cached_empty_content_header_frame_body_size : UInt64?
    @body_frame_prefix_candidate_size : Int32?
    @cached_body_frame_prefix : Bytes?
    @cached_body_frame_prefix_size : Int32?

    protected def initialize(@connection : Connection, @id : UInt16)
      @state = State::Initial
      @inbox = ::Channel(Amqp::Wire::Frame).new(256)
      @sync_mutex = Mutex.new
      @operation_mutex = Mutex.new
      @operation_busy = false
      @flow_mutex = Mutex.new
      @flow_active = true
      @flow_waker = ::Channel(Nil).new
      @tx_enabled = false
      @sync_slot = nil
      @get_slot = nil
      @consumers = {} of String => Subscription
      @consumers_mutex = Mutex.new
      @consumers_generation = Atomic(Int64).new(0_i64)
      @cached_consumer_tag = nil
      @cached_consumer = nil
      @cached_consumer_generation = -1_i64
      @cached_deliver_consumer_tag = nil
      @cached_deliver_exchange = nil
      @cached_deliver_routing_key = nil
      @pending_method = nil
      @pending_props = nil
      @pending_body_size = 0_u64
      @pending_body_received = 0_u64
      @pending_body = IO::Memory.new
      @pending_body_direct = nil
      @close_reason = nil
      @handler_done = ::Channel(Nil).new
      @confirms_enabled = false
      @confirms_mutex = Mutex.new
      @next_publish_seq = 1_u64
      @unconfirmed = Set(UInt64).new
      @lowest_unconfirmed = nil
      @pending_confirms = {} of UInt64 => PendingConfirm
      @returned_confirms = {} of UInt64 => ReturnReason
      @completed_sync_confirm_tag = nil
      @completed_sync_confirm_outcome = nil
      @completed_sync_confirms = {} of UInt64 => ConfirmOutcome
      @confirms_nacked = false
      @confirms_waker = ::Channel(Nil).new
      @confirm_callback_queue = ::Channel(Tuple(ConfirmCallback, Bool)).new(1024)
      @on_return = nil
      @on_cancel = nil
      @on_close = nil
      @topology_mutex = Mutex.new
      @topology_exchanges = {} of String => ExchangeOp
      @topology_queues = {} of String => QueueOp
      @topology_bindings = [] of BindOp
      @topology_qos = nil
      @topology_consumers = {} of String => ConsumeOp
      @publish_frame_candidate_exchange = nil
      @publish_frame_candidate_routing_key = nil
      @publish_frame_candidate_mandatory = false
      @publish_frame_candidate_immediate = false
      @cached_publish_method_frame = nil
      @cached_publish_method_frame_exchange = nil
      @cached_publish_method_frame_routing_key = nil
      @cached_publish_method_frame_mandatory = false
      @cached_publish_method_frame_immediate = false
      @empty_header_candidate_body_size = nil
      @cached_empty_content_header_frame = nil
      @cached_empty_content_header_frame_body_size = nil
      @body_frame_prefix_candidate_size = nil
      @cached_body_frame_prefix = nil
      @cached_body_frame_prefix_size = nil
    end

    protected def open : Nil
      spawn(name: "amqp-channel-#{@id}") { run_handler }
      spawn(name: "amqp-confirm-callbacks-#{@id}") { run_confirm_callback_loop }
      begin
        env = sync_rpc(Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Open.new.to_payload)
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CHANNEL,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CHANNEL_OPEN_OK)
        Amqp::Wire::AmqpZeroNineOne::ChannelMethods::OpenOk.read(env.body)
        @state = State::Open
      rescue ex
        finalize_closed(ex)
        raise ex
      end
    end

    def open? : Bool
      @state == State::Open
    end

    def closed? : Bool
      @state == State::Closed
    end

    def close(*, reply_code : UInt16 = 200_u16, reply_text : String = "OK") : Nil
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

    def close_reason : Exception?
      @close_reason
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

    def exchange_delete(name : String, *, if_unused : Bool = false) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::ExchangeMethods::Delete.new(name, if_unused).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_EXCHANGE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_EXCHANGE_DELETE_OK)
      @topology_mutex.synchronize { @topology_exchanges.delete(name) }
    end

    def exchange_bind(destination : String,
                      source : String,
                      routing_key : String = "",
                      arguments : Amqp::Arguments = Amqp::Arguments.new) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::ExchangeMethods::Bind.new(
          destination, source, routing_key, arguments,
        ).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_EXCHANGE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_EXCHANGE_BIND_OK)
    end

    def exchange_unbind(destination : String,
                        source : String,
                        routing_key : String = "",
                        arguments : Amqp::Arguments = Amqp::Arguments.new) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::ExchangeMethods::Unbind.new(
          destination, source, routing_key, arguments,
        ).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_EXCHANGE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_EXCHANGE_UNBIND_OK)
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

    # :nodoc:
    def __compat_queue_declare_no_wait(name : String = "",
                                       passive : Bool = false,
                                       durable : Bool = false,
                                       exclusive : Bool = false,
                                       auto_delete : Bool = false,
                                       arguments : Amqp::Arguments = Amqp::Arguments.new) : QueueInfo
      ensure_open!
      payload = IO::Memory.new
      payload.write_bytes(Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
        IO::ByteFormat::NetworkEndian)
      payload.write_bytes(Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_DECLARE,
        IO::ByteFormat::NetworkEndian)
      payload.write_bytes(0_u16, IO::ByteFormat::NetworkEndian)
      Amqp::Wire::AmqpZeroNineOne::Types.write_shortstr(payload, name)
      Amqp::Wire::AmqpZeroNineOne::BitPack.write(payload,
        [passive, durable, exclusive, auto_delete, true])
      Amqp::Wire::AmqpZeroNineOne::Types.write_field_table(payload, arguments)
      @connection.write_frame(@id, Amqp::Wire::FrameType::Method, payload.to_slice)
      QueueInfo.new(name, 0_u32, 0_u32)
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

    def queue_unbind(queue : String,
                     exchange : String,
                     routing_key : String = "",
                     arguments : Amqp::Arguments = Amqp::Arguments.new) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::QueueMethods::Unbind.new(
          queue, exchange, routing_key, arguments,
        ).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_UNBIND_OK)
      @topology_mutex.synchronize do
        @topology_bindings.reject! { |b| b.queue == queue && b.exchange == exchange && b.routing_key == routing_key }
      end
    end

    def queue_purge(name : String) : UInt32
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::QueueMethods::Purge.new(name).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_QUEUE,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_QUEUE_PURGE_OK)
      Amqp::Wire::AmqpZeroNineOne::QueueMethods::PurgeOk.read(env.body).message_count
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

    def prefetch(count : UInt16, *, global : Bool = false) : Nil
      qos(count, global: global)
    end

    def publish(exchange : String,
                routing_key : String,
                body : Bytes,
                properties : Properties = Properties.new,
                mandatory : Bool = false,
                immediate : Bool = false) : UInt64?
      ensure_open!
      wait_for_flow_active
      return publish_unconfirmed(body, properties, exchange, routing_key, mandatory, immediate) unless @confirms_enabled

      publish(Message.new(body, properties), exchange, routing_key,
        mandatory: mandatory, immediate: immediate)
    end

    def publish(message : Message,
                exchange : String,
                routing_key : String,
                *,
                mandatory : Bool = false,
                immediate : Bool = false) : UInt64?
      ensure_open!
      wait_for_flow_active
      return publish_unconfirmed(message, exchange, routing_key, mandatory, immediate) unless @confirms_enabled

      enter_operation
      begin
        publish_registered(message, exchange, routing_key, mandatory, immediate, nil)
      ensure
        leave_operation
      end
    end

    def publish_batch(messages : Array(Message),
                      exchange : String,
                      routing_key : String,
                      *,
                      mandatory : Bool = false,
                      immediate : Bool = false) : Array(UInt64?)
      ensure_open!
      wait_for_flow_active
      return publish_batch_unconfirmed(messages, exchange, routing_key, mandatory, immediate) unless @confirms_enabled

      enter_operation
      begin
        publish_batch_registered(messages, exchange, routing_key, mandatory, immediate)
      ensure
        leave_operation
      end
    end

    def publish_batch(bodies : Array(Bytes),
                      exchange : String,
                      routing_key : String,
                      *,
                      properties : Properties = Properties.new,
                      mandatory : Bool = false,
                      immediate : Bool = false) : Array(UInt64?)
      ensure_open!
      wait_for_flow_active
      return publish_batch_unconfirmed(bodies, exchange, routing_key, properties, mandatory, immediate) unless @confirms_enabled

      enter_operation
      begin
        publish_batch_registered(bodies, exchange, routing_key, properties, mandatory, immediate)
      ensure
        leave_operation
      end
    end

    def publish_confirm_batch(messages : Array(Message),
                              exchange : String,
                              routing_key : String,
                              *,
                              window_size : Int32 = 500,
                              mandatory : Bool = false,
                              immediate : Bool = false,
                              timeout : Time::Span = 30.seconds) : Bool
      raise ConfigurationError.new("publish_confirm_batch requires confirm mode") unless @confirms_enabled
      raise ArgumentError.new("window_size must be positive") unless window_size > 0
      return true if messages.empty?

      index = 0
      while index < messages.size
        count = Math.min(window_size, messages.size - index)
        ensure_open!
        wait_for_flow_active
        enter_operation
        begin
          publish_batch_registered_range(messages, index, count, exchange, routing_key, mandatory, immediate)
        ensure
          leave_operation
        end
        return false unless wait_for_confirms(timeout)
        index += count
      end
      true
    end

    def publish_confirm_batch(bodies : Array(Bytes),
                              exchange : String,
                              routing_key : String,
                              *,
                              properties : Properties = Properties.new,
                              window_size : Int32 = 500,
                              mandatory : Bool = false,
                              immediate : Bool = false,
                              timeout : Time::Span = 30.seconds) : Bool
      raise ConfigurationError.new("publish_confirm_batch requires confirm mode") unless @confirms_enabled
      raise ArgumentError.new("window_size must be positive") unless window_size > 0
      return true if bodies.empty?

      index = 0
      while index < bodies.size
        count = Math.min(window_size, bodies.size - index)
        ensure_open!
        wait_for_flow_active
        enter_operation
        begin
          publish_batch_registered_range(bodies, index, count, exchange, routing_key,
            properties, mandatory, immediate)
        ensure
          leave_operation
        end
        return false unless wait_for_confirms(timeout)
        index += count
      end
      true
    end

    def prepared_publisher(exchange : String,
                           routing_key : String,
                           *,
                           properties : Properties = Properties.new,
                           mandatory : Bool = false,
                           immediate : Bool = false) : PreparedPublisher
      PreparedPublisher.new(
        self,
        exchange,
        routing_key,
        properties,
        mandatory,
        immediate,
        publish_method_frame(exchange, routing_key, mandatory, immediate),
      )
    end

    def publish_confirm(message : Message,
                        exchange : String,
                        routing_key : String,
                        *,
                        mandatory : Bool = false,
                        timeout : Time::Span = 30.seconds) : Bool
      raise ConfigurationError.new("publish_confirm requires confirm mode") unless @confirms_enabled
      raise PublishTimeoutError.new(0_u64, timeout) if timeout <= Time::Span.zero
      wait_for_flow_active
      tag = publish_registered_sync(message, exchange, routing_key, mandatory, false)
      await_publish_confirm(tag.not_nil!, exchange, routing_key, timeout)
    end

    private def await_publish_confirm(tag : UInt64,
                                      exchange : String,
                                      routing_key : String,
                                      timeout : Time::Span) : Bool
      deadline = Time.instant + timeout
      loop do
        waker = @confirms_mutex.synchronize do
          if result = take_completed_sync_confirm_locked(tag)
            return handle_publish_confirm_result(result, exchange, routing_key)
          end

          unless @pending_confirms.has_key?(tag)
            if reason = @close_reason
              raise reason
            end
          end

          @confirms_waker
        end

        remaining = deadline - Time.instant
        if remaining <= Time::Span.zero
          result = @confirms_mutex.synchronize do
            take_completed_sync_confirm_locked(tag).tap do |outcome|
              abandon_sync_confirm_waiter_locked(tag) unless outcome
            end
          end
          return handle_publish_confirm_result(result, exchange, routing_key) if result
          raise PublishTimeoutError.new(tag, timeout)
        end

        select
        when waker.receive?
        when timeout(remaining)
        end
      end
    end

    private def handle_publish_confirm_result(result : ConfirmOutcome,
                                              exchange : String,
                                              routing_key : String) : Bool
      case result.kind
      when .ack?
        true
      when .nack?
        raise PublishNackError.new(result.delivery_tag)
      when .returned?
        reason = result.return_reason || ReturnReason.new(0_u16, "returned", exchange, routing_key)
        raise PublishReturnedError.new(result.delivery_tag, reason)
      else
        raise ProtocolError.new("unknown confirm outcome #{result.kind}")
      end
    end

    def publish_confirm(body : Bytes,
                        exchange : String,
                        routing_key : String,
                        *,
                        properties : Properties = Properties.new,
                        mandatory : Bool = false,
                        timeout : Time::Span = 30.seconds) : Bool
      raise ConfigurationError.new("publish_confirm requires confirm mode") unless @confirms_enabled
      raise PublishTimeoutError.new(0_u64, timeout) if timeout <= Time::Span.zero
      wait_for_flow_active
      tag = publish_registered_sync(body, properties, exchange, routing_key, mandatory, false)
      await_publish_confirm(tag.not_nil!, exchange, routing_key, timeout)
    end

    def publish_async(message : Message,
                      exchange : String,
                      routing_key : String,
                      *,
                      mandatory : Bool = false) : {UInt64, ::Channel(ConfirmOutcome)}
      raise ConfigurationError.new("publish_async requires confirm mode") unless @confirms_enabled
      wait_for_flow_active
      outcome = ::Channel(ConfirmOutcome).new(1)
      enter_operation
      tag = begin
        publish_registered(message, exchange, routing_key, mandatory, false, outcome)
      ensure
        leave_operation
      end
      {tag.not_nil!, outcome}
    end

    def publish_async(body : Bytes,
                      exchange : String,
                      routing_key : String,
                      *,
                      properties : Properties = Properties.new,
                      mandatory : Bool = false) : {UInt64, ::Channel(ConfirmOutcome)}
      publish_async(Message.new(body, properties), exchange, routing_key, mandatory: mandatory)
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
        @lowest_unconfirmed = nil
        @pending_confirms.clear
        @returned_confirms.clear
        @confirms_nacked = false
      end
    end

    def confirms? : Bool
      @confirms_enabled
    end

    def confirms_enabled? : Bool
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

      deadline = Time.instant + timeout
      loop do
        settled, nacked, waker = @confirms_mutex.synchronize do
          done = @lowest_unconfirmed.nil? || @lowest_unconfirmed.not_nil! > target_seq
          {done, @confirms_nacked, @confirms_waker}
        end
        return !nacked if settled
        if @state == State::Closed
          raise(@close_reason || ChannelClosedByCaller.new("channel #{@id} closed"))
        end
        remaining = deadline - Time.instant
        return false if remaining <= Time::Span.zero
        select
        when waker.receive?
          # signaled — re-check on next iteration
        when timeout(remaining)
          return false
        end
      end
    end

    def flow(active : Bool) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Flow.new(active).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CHANNEL,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CHANNEL_FLOW_OK)
      Amqp::Wire::AmqpZeroNineOne::ChannelMethods::FlowOk.read(env.body)
    end

    def on_return(&callback : ReturnedMessage -> Nil) : Nil
      @on_return = callback
    end

    def on_cancel(&callback : String -> Nil) : Nil
      @on_cancel = callback
    end

    def on_close(&callback : UInt16, String -> Nil) : Nil
      @on_close = callback
    end

    private def read_publish_body(io : IO, bytesize : Int) : Bytes
      raise ArgumentError.new("bytesize must be non-negative") if bytesize < 0

      body = Bytes.new(bytesize)
      offset = 0
      while offset < bytesize
        read = io.read(body[offset, bytesize - offset])
        raise IO::EOFError.new("unexpected EOF while reading publish body") if read == 0
        offset += read
      end
      body
    end

    private def read_publish_body_to_end(io : IO) : Bytes
      buffer = IO::Memory.new
      IO.copy(io, buffer)
      buffer.to_slice
    end

    # ---- amqp-client.cr compatibility aliases --------------------------

    def basic_publish(body : Bytes,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new) : UInt64
      publish(exchange, routing_key, body, properties,
        mandatory: mandatory, immediate: immediate) || 0_u64
    end

    def basic_publish(body : String,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new) : UInt64
      basic_publish(body.to_slice, exchange, routing_key, mandatory, immediate, properties)
    end

    def basic_publish(io : IO,
                      bytesize : Int,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new) : UInt64
      basic_publish(read_publish_body(io, bytesize), exchange, routing_key,
        mandatory, immediate, properties)
    end

    def basic_publish(io : IO,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new) : UInt64
      basic_publish(read_publish_body_to_end(io), exchange, routing_key,
        mandatory, immediate, properties)
    end

    def basic_publish(body : Bytes,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new,
                      &callback : Bool -> Nil) : UInt64
      raise ConfigurationError.new("basic_publish callback mode does not support immediate: true") if immediate
      confirm_select unless @confirms_enabled
      ensure_open!
      wait_for_flow_active
      publish_registered_callback(Message.new(body, properties), exchange, routing_key,
        mandatory, callback)
    end

    def basic_publish(io : IO,
                      bytesize : Int,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new,
                      &callback : Bool -> Nil) : UInt64
      basic_publish(read_publish_body(io, bytesize), exchange, routing_key,
        mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def basic_publish(io : IO,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new,
                      &callback : Bool -> Nil) : UInt64
      basic_publish(read_publish_body_to_end(io), exchange, routing_key,
        mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def basic_publish(body : String,
                      exchange : String,
                      routing_key : String = "",
                      mandatory : Bool = false,
                      immediate : Bool = false,
                      props properties : Properties = Properties.new,
                      &callback : Bool -> Nil) : UInt64
      basic_publish(body.to_slice, exchange, routing_key, mandatory, immediate, properties) do |ok|
        callback.call(ok)
      end
    end

    def basic_publish_confirm(body : Bytes,
                              exchange : String,
                              routing_key : String = "",
                              mandatory : Bool = false,
                              immediate : Bool = false,
                              props properties : Properties = Properties.new,
                              timeout : Time::Span = 30.seconds) : Bool
      raise ConfigurationError.new("basic_publish_confirm does not support immediate: true") if immediate
      confirm_select unless @confirms_enabled
      publish_confirm(Message.new(body, properties), exchange, routing_key,
        mandatory: mandatory, timeout: timeout)
    end

    def basic_publish_confirm(body : String,
                              exchange : String,
                              routing_key : String = "",
                              mandatory : Bool = false,
                              immediate : Bool = false,
                              props properties : Properties = Properties.new,
                              timeout : Time::Span = 30.seconds) : Bool
      basic_publish_confirm(body.to_slice, exchange, routing_key, mandatory, immediate,
        properties, timeout: timeout)
    end

    def basic_publish_confirm(io : IO,
                              bytesize : Int,
                              exchange : String,
                              routing_key : String = "",
                              mandatory : Bool = false,
                              immediate : Bool = false,
                              props properties : Properties = Properties.new,
                              timeout : Time::Span = 30.seconds) : Bool
      basic_publish_confirm(read_publish_body(io, bytesize), exchange, routing_key,
        mandatory, immediate, properties, timeout: timeout)
    end

    def basic_publish_confirm(io : IO,
                              exchange : String,
                              routing_key : String = "",
                              mandatory : Bool = false,
                              immediate : Bool = false,
                              props properties : Properties = Properties.new,
                              timeout : Time::Span = 30.seconds) : Bool
      basic_publish_confirm(read_publish_body_to_end(io), exchange, routing_key,
        mandatory, immediate, properties, timeout: timeout)
    end

    def basic_get(queue : String, no_ack : Bool = true) : GetMessage?
      get(queue, auto_ack: no_ack)
    end

    def basic_consume(queue : String,
                      tag : String = "",
                      no_ack : Bool = true,
                      exclusive : Bool = false,
                      block : Bool = false,
                      args arguments : Arguments = Arguments.new,
                      work_pool : Int32 = 1,
                      &callback : DeliverMessage -> Nil) : String
      raise ArgumentError.new("Max allowed work_pool is 1024") if work_pool > 1024
      raise ArgumentError.new("At least one worker required") if work_pool < 1

      sub = subscribe(queue, consumer_tag: tag, auto_ack: no_ack,
        exclusive: exclusive, arguments: arguments)
      done = ::Channel(Exception?).new(work_pool)
      work_pool.times do |index|
        spawn(name: "amqp-basic-consume-#{sub.consumer_tag}-#{index}") do
          begin
            sub.each do |delivery|
              callback.call(delivery)
            end
            done.send(nil)
          rescue ex
            close(reply_code: 500_u16, reply_text: "uncaught consumer exception #{sub.consumer_tag}") rescue nil
            done.send(ex) rescue nil
          end
        end
      end

      if block
        work_pool.times do
          if ex = done.receive
            raise ex
          end
        end
      end
      sub.consumer_tag
    end

    def basic_cancel(consumer_tag : String, no_wait : Bool = false) : Nil
      cancel(consumer_tag)
    end

    def basic_ack(delivery_tag : UInt64, multiple : Bool = false) : Nil
      ack(delivery_tag, multiple: multiple)
    end

    def basic_reject(delivery_tag : UInt64, requeue : Bool = false) : Nil
      reject(delivery_tag, requeue: requeue)
    end

    def basic_nack(delivery_tag : UInt64, requeue : Bool = false, multiple : Bool = false) : Nil
      nack(delivery_tag, multiple: multiple, requeue: requeue)
    end

    def basic_qos(count : UInt16, global : Bool = false) : Nil
      qos(count, global: global)
    end

    def basic_recover(requeue : Bool = true) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::BasicMethods::Recover.new(requeue).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_RECOVER_OK)
      Amqp::Wire::AmqpZeroNineOne::BasicMethods::RecoverOk.read(env.body)
    end

    def tx_select : Nil
      return if @tx_enabled
      env = sync_rpc(Amqp::Wire::AmqpZeroNineOne::TxMethods::Select.new.to_payload)
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_TX,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_TX_SELECT_OK)
      Amqp::Wire::AmqpZeroNineOne::TxMethods::SelectOk.read(env.body)
      @tx_enabled = true
    end

    def tx_commit : Nil
      env = sync_rpc(Amqp::Wire::AmqpZeroNineOne::TxMethods::Commit.new.to_payload)
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_TX,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_TX_COMMIT_OK)
      Amqp::Wire::AmqpZeroNineOne::TxMethods::CommitOk.read(env.body)
    end

    def tx_rollback : Nil
      env = sync_rpc(Amqp::Wire::AmqpZeroNineOne::TxMethods::Rollback.new.to_payload)
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_TX,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_TX_ROLLBACK_OK)
      Amqp::Wire::AmqpZeroNineOne::TxMethods::RollbackOk.read(env.body)
    end

    def transaction(& : -> T) : T forall T
      tx_select
      begin
        value = yield
      rescue ex
        tx_rollback rescue nil
        raise ex
      else
        tx_commit
        value
      end
    end

    def queue : Queue
      info = queue_declare("", durable: false, exclusive: true, auto_delete: true)
      Queue.new(self, info.name)
    end

    def queue(name : String,
              passive : Bool = false,
              durable : Bool = true,
              exclusive : Bool = false,
              auto_delete : Bool = false,
              args arguments : Arguments = Arguments.new) : Queue
      info = queue_declare(name, passive, durable, exclusive, auto_delete, arguments)
      Queue.new(self, info.name)
    end

    def exchange(name : String,
                 type : String,
                 passive : Bool = false,
                 durable : Bool = true,
                 internal : Bool = false,
                 auto_delete : Bool = false,
                 args arguments : Arguments = Arguments.new) : Exchange
      exchange_declare(name, type, passive, durable,
        auto_delete: auto_delete, internal: internal, arguments: arguments)
      Exchange.new(self, name)
    end

    def default_exchange : Exchange
      Exchange.new(self, "")
    end

    def direct_exchange(name : String = "amq.direct", passive : Bool = true) : Exchange
      exchange(name, "direct", passive)
    end

    def topic_exchange(name : String = "amq.topic", passive : Bool = true) : Exchange
      exchange(name, "topic", passive)
    end

    def fanout_exchange(name : String = "amq.fanout", passive : Bool = true) : Exchange
      exchange(name, "fanout", passive)
    end

    def header_exchange(name : String = "amq.headers", passive : Bool = true) : Exchange
      exchange(name, "headers", passive)
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
                arguments : Amqp::Arguments = Amqp::Arguments.new,
                buffer : Int32 = 1024) : Subscription
      # Pre-generate a client-side tag if caller didn't supply one. This
      # lets us register the Subscription BEFORE the broker can send any
      # basic.deliver — otherwise a fast broker (or anything that races
      # the handler fiber) could route a delivery against an unknown tag.
      tag = consumer_tag.empty? ? "amqp-ng-ctag-#{Random::Secure.hex(8)}" : consumer_tag
      sub = Subscription.new(self, tag, queue, buffer)
      register_consumer(tag, sub)

      begin
        env = sync_rpc(
          Amqp::Wire::AmqpZeroNineOne::BasicMethods::Consume.new(
            queue, tag, no_local, no_ack, exclusive, arguments,
          ).to_payload
        )
      rescue ex
        delete_consumer(tag)
        sub.mark_closed
        raise ex
      end

      begin
        expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CONSUME_OK)
        Amqp::Wire::AmqpZeroNineOne::BasicMethods::ConsumeOk.read(env.body)
      rescue ex
        delete_consumer(tag)
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

    def subscribe(queue : String,
                  *,
                  consumer_tag : String = "",
                  auto_ack : Bool = false,
                  exclusive : Bool = false,
                  no_local : Bool = false,
                  arguments : Amqp::Arguments = Amqp::Arguments.new,
                  buffer : Int32 = 16) : Subscription
      consume(queue, consumer_tag: consumer_tag, no_local: no_local,
        no_ack: auto_ack, exclusive: exclusive, arguments: arguments, buffer: buffer)
    end

    def consume(queue : String,
                *,
                consumer_tag : String = "",
                auto_ack : Bool = false,
                exclusive : Bool = false,
                no_local : Bool = false,
                arguments : Amqp::Arguments = Amqp::Arguments.new,
                & : DeliverMessage -> _) : Nil
      sub = subscribe(queue, consumer_tag: consumer_tag, auto_ack: auto_ack,
        exclusive: exclusive, no_local: no_local, arguments: arguments)
      loop do
        delivery = sub.receive
        begin
          yield delivery
        rescue ex
          Log.warn(exception: ex) { "auto_ack consumer block failed after broker-side ack" } if auto_ack
          delivery.reject(requeue: true) unless auto_ack
          raise ex
        end
      rescue Subscription::Closed
        break
      end
    end

    def get(queue : String, *, auto_ack : Bool = false) : GetMessage?
      ensure_open!
      enter_operation
      slot = ::Channel(GetMessage | Nil | Exception).new(1)
      @get_slot = slot
      begin
        @connection.write_frame(@id, Amqp::Wire::FrameType::Method,
          Amqp::Wire::AmqpZeroNineOne::BasicMethods::Get.new(queue, auto_ack).to_payload)
        reply = receive_get_reply(slot, default_rpc_timeout)
        case reply
        in Exception
          raise reply
        in GetMessage
          reply
        in Nil
          nil
        end
      ensure
        @get_slot = nil
        leave_operation
      end
    end

    def cancel(consumer_tag : String) : Nil
      env = sync_rpc(
        Amqp::Wire::AmqpZeroNineOne::BasicMethods::Cancel.new(consumer_tag).to_payload
      )
      expect_method!(env, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
        Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CANCEL_OK)
      sub = delete_consumer(consumer_tag)
      @topology_mutex.synchronize { @topology_consumers.delete(consumer_tag) }
      sub.try &.mark_closed
    end

    def ack(delivery_tag : UInt64, multiple : Bool = false) : Nil
      ensure_open!
      @connection.with_write do |io|
        Amqp::Wire::AmqpZeroNineOne::BasicMethods.write_ack_frame(
          io, @id, delivery_tag, multiple,
        )
      end
    end

    def nack(delivery_tag : UInt64, multiple : Bool = false, requeue : Bool = true) : Nil
      ensure_open!
      @connection.with_write do |io|
        Amqp::Wire::AmqpZeroNineOne::BasicMethods.write_nack_frame(
          io, @id, delivery_tag, multiple, requeue,
        )
      end
    end

    def reject(delivery_tag : UInt64, requeue : Bool = true) : Nil
      ensure_open!
      @connection.with_write do |io|
        Amqp::Wire::AmqpZeroNineOne::BasicMethods.write_reject_frame(
          io, @id, delivery_tag, requeue,
        )
      end
    end

    # ---- Internals -----------------------------------------------------

    private def default_rpc_timeout : Time::Span
      hb = @connection.heartbeat
      hb == Time::Span.zero ? 5.seconds : hb
    end

    private def enter_operation : Nil
      @operation_mutex.synchronize do
        if @operation_busy
          raise ConcurrencyError.new("channel #{@id} already has a state-changing operation in progress")
        end
        @operation_busy = true
      end
    end

    private def leave_operation : Nil
      @operation_mutex.synchronize { @operation_busy = false }
    end

    private def receive_sync_reply(slot, timeout : Time::Span) : MethodEnvelope | Exception
      select
      when reply = slot.receive
        reply
      when timeout(timeout)
        ChannelRpcTimeoutError.new("channel #{@id}: timed out waiting for broker reply")
      end
    end

    private def receive_get_reply(slot, timeout : Time::Span) : GetMessage | Nil | Exception
      select
      when reply = slot.receive
        reply
      when timeout(timeout)
        ChannelRpcTimeoutError.new("channel #{@id}: timed out waiting for basic.get reply")
      end
    end

    private def wait_for_flow_active : Nil
      loop do
        ensure_open!
        waker = @flow_mutex.synchronize do
          return if @flow_active
          @flow_waker
        end
        select
        when waker.receive?
        when timeout(default_rpc_timeout)
        end
      end
    end

    private def set_flow_active(active : Bool) : Nil
      old_waker = nil
      @flow_mutex.synchronize do
        @flow_active = active
        if active
          old_waker = @flow_waker
          @flow_waker = ::Channel(Nil).new
        end
      end
      old_waker.try &.close
    end

    private def publish_unconfirmed(message : Message,
                                    exchange : String,
                                    routing_key : String,
                                    mandatory : Bool,
                                    immediate : Bool) : UInt64?
      header_payload = unless message.properties.empty?
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
          Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          message.body.size.to_u64,
          message.properties,
        )
      end
      write_publish_frames(exchange, routing_key, mandatory, immediate,
        header_payload, message.body, max_body_per_frame(@connection.frame_max)) { }
      @connection.stats.incr_published
      nil
    end

    private def publish_unconfirmed(body : Bytes,
                                    properties : Properties,
                                    exchange : String,
                                    routing_key : String,
                                    mandatory : Bool,
                                    immediate : Bool) : UInt64?
      header_payload = unless properties.empty?
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
          Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          body.size.to_u64,
          properties,
        )
      end
      write_publish_frames(exchange, routing_key, mandatory, immediate,
        header_payload, body, max_body_per_frame(@connection.frame_max)) { }
      @connection.stats.incr_published
      nil
    end

    # :nodoc:
    def __publish_prepared(prepared : PreparedPublisher, body : Bytes) : UInt64?
      ensure_open!
      wait_for_flow_active
      if @confirms_enabled
        return publish(Message.new(body, prepared.properties),
          prepared.exchange, prepared.routing_key,
          mandatory: prepared.mandatory, immediate: prepared.immediate)
      end

      publish_prepared_unconfirmed(prepared, body)
    end

    # :nodoc:
    def __publish_prepared_batch(prepared : PreparedPublisher, bodies : Array(Bytes)) : Array(UInt64?)
      ensure_open!
      wait_for_flow_active
      if @confirms_enabled
        return publish_batch(bodies, prepared.exchange, prepared.routing_key,
          properties: prepared.properties,
          mandatory: prepared.mandatory,
          immediate: prepared.immediate)
      end

      publish_prepared_batch_unconfirmed(prepared, bodies)
    end

    private def publish_prepared_unconfirmed(prepared : PreparedPublisher,
                                             body : Bytes) : UInt64?
      write_prepared_publish_frames(prepared, body,
        max_body_per_frame(@connection.frame_max)) { }
      @connection.stats.incr_published
      nil
    end

    private def publish_prepared_batch_unconfirmed(prepared : PreparedPublisher,
                                                   bodies : Array(Bytes)) : Array(UInt64?)
      seqs = Array(UInt64?).new(bodies.size) { nil }
      return seqs if bodies.empty?

      max_body = max_body_per_frame(@connection.frame_max)
      @connection.with_write do |io|
        if prepared.properties.empty?
          bodies.each do |body|
            write_publish_frames_to(io, prepared.method_frame, nil, body, max_body)
          end
        else
          bodies.each do |body|
            write_prepared_publish_frames_to(io, prepared, body, max_body)
          end
        end
      end
      @connection.stats.incr_published(bodies.size.to_i64)
      seqs
    end

    private def publish_registered(message : Message,
                                   exchange : String,
                                   routing_key : String,
                                   mandatory : Bool,
                                   immediate : Bool,
                                   outcome : ::Channel(ConfirmOutcome)?) : UInt64?
      header_payload = unless message.properties.empty?
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
          Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          message.body.size.to_u64,
          message.properties,
        )
      end
      frame_max = @connection.frame_max
      max_body = max_body_per_frame(frame_max)
      seq = nil
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      @confirms_mutex.lock if lock_during_write
      begin
        if @confirms_enabled
          if lock_during_write
            seq = register_pending_confirm_locked(message, exchange, routing_key, mandatory, outcome)
          else
            write_publish_frames(exchange, routing_key, mandatory, immediate, header_payload,
              message.body, max_body) do
              seq = @confirms_mutex.synchronize do
                register_pending_confirm_locked(message, exchange, routing_key, mandatory, outcome)
              end
            end
            @connection.stats.incr_published
            return seq
          end
        end
        write_publish_frames(exchange, routing_key, mandatory, immediate, header_payload,
          message.body, max_body) { }
      rescue ex
        if tag = seq
          if lock_during_write
            discard_pending_confirm_locked(tag)
          else
            @confirms_mutex.synchronize { discard_pending_confirm_locked(tag) }
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published
      seq
    end

    private def publish_registered_sync(message : Message,
                                        exchange : String,
                                        routing_key : String,
                                        mandatory : Bool,
                                        immediate : Bool) : UInt64?
      header_payload = unless message.properties.empty?
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
          Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          message.body.size.to_u64,
          message.properties,
        )
      end
      frame_max = @connection.frame_max
      max_body = max_body_per_frame(frame_max)
      seq = nil
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      @confirms_mutex.lock if lock_during_write
      begin
        if lock_during_write
          seq = register_sync_pending_confirm_locked(message, exchange, routing_key, mandatory)
        else
          write_publish_frames(exchange, routing_key, mandatory, immediate, header_payload,
            message.body, max_body) do
            seq = @confirms_mutex.synchronize do
              register_sync_pending_confirm_locked(message, exchange, routing_key, mandatory)
            end
          end
          @connection.stats.incr_published
          return seq
        end
        write_publish_frames(exchange, routing_key, mandatory, immediate, header_payload,
          message.body, max_body) { }
      rescue ex
        if tag = seq
          if lock_during_write
            discard_pending_confirm_locked(tag)
          else
            @confirms_mutex.synchronize { discard_pending_confirm_locked(tag) }
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published
      seq
    end

    private def publish_registered_sync(body : Bytes,
                                        properties : Properties,
                                        exchange : String,
                                        routing_key : String,
                                        mandatory : Bool,
                                        immediate : Bool) : UInt64?
      header_payload = unless properties.empty?
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
          Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          body.size.to_u64,
          properties,
        )
      end
      frame_max = @connection.frame_max
      max_body = max_body_per_frame(frame_max)
      seq = nil
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      @confirms_mutex.lock if lock_during_write
      begin
        if lock_during_write
          replay_message = Message.new(body, properties)
          seq = register_sync_pending_confirm_replay_locked(replay_message,
            exchange, routing_key, mandatory)
        else
          write_publish_frames(exchange, routing_key, mandatory, immediate, header_payload,
            body, max_body) do
            seq = @confirms_mutex.synchronize do
              register_sync_pending_confirm_replay_locked(nil, exchange, routing_key, mandatory)
            end
          end
          @connection.stats.incr_published
          return seq
        end
        write_publish_frames(exchange, routing_key, mandatory, immediate, header_payload,
          body, max_body) { }
      rescue ex
        if tag = seq
          if lock_during_write
            discard_pending_confirm_locked(tag)
          else
            @confirms_mutex.synchronize { discard_pending_confirm_locked(tag) }
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published
      seq
    end

    private def publish_registered_callback(message : Message,
                                            exchange : String,
                                            routing_key : String,
                                            mandatory : Bool,
                                            callback : ConfirmCallback) : UInt64
      header_payload = unless message.properties.empty?
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
          Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
          message.body.size.to_u64,
          message.properties,
        )
      end
      frame_max = @connection.frame_max
      max_body = max_body_per_frame(frame_max)
      seq = nil
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      @confirms_mutex.lock if lock_during_write
      begin
        if lock_during_write
          seq = register_callback_pending_confirm_locked(message, exchange, routing_key,
            mandatory, callback)
        else
          write_publish_frames(exchange, routing_key, mandatory, false, header_payload,
            message.body, max_body) do
            seq = @confirms_mutex.synchronize do
              register_callback_pending_confirm_locked(message, exchange, routing_key,
                mandatory, callback)
            end
          end
          @connection.stats.incr_published
          return seq.not_nil!
        end
        write_publish_frames(exchange, routing_key, mandatory, false, header_payload,
          message.body, max_body) { }
      rescue ex
        if tag = seq
          if lock_during_write
            discard_pending_confirm_locked(tag)
          else
            @confirms_mutex.synchronize { discard_pending_confirm_locked(tag) }
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published
      seq.not_nil!
    end

    private def register_pending_confirm_locked(message : Message,
                                                exchange : String,
                                                routing_key : String,
                                                mandatory : Bool,
                                                outcome : ::Channel(ConfirmOutcome)?) : UInt64
      replay_message = @connection.recovery_mode.full? ? message : nil
      register_pending_confirm_replay_locked(replay_message, exchange, routing_key,
        mandatory, outcome)
    end

    private def register_pending_confirm_replay_locked(replay_message : Message?,
                                                       exchange : String,
                                                       routing_key : String,
                                                       mandatory : Bool,
                                                       outcome : ::Channel(ConfirmOutcome)?) : UInt64
      seq = @next_publish_seq
      @next_publish_seq += 1
      @unconfirmed << seq
      @lowest_unconfirmed ||= seq
      @pending_confirms[seq] = PendingConfirm.new(
        seq, mandatory, outcome, nil, replay_message, exchange, routing_key, false,
      )
      seq
    end

    private def register_callback_pending_confirm_locked(message : Message,
                                                         exchange : String,
                                                         routing_key : String,
                                                         mandatory : Bool,
                                                         callback : ConfirmCallback) : UInt64
      seq = @next_publish_seq
      @next_publish_seq += 1
      @unconfirmed << seq
      @lowest_unconfirmed ||= seq
      replay_message = @connection.recovery_mode.full? ? message : nil
      @pending_confirms[seq] = PendingConfirm.new(
        seq, mandatory, nil, callback, replay_message, exchange, routing_key, false,
      )
      seq
    end

    private def register_sync_pending_confirm_locked(message : Message,
                                                     exchange : String,
                                                     routing_key : String,
                                                     mandatory : Bool) : UInt64
      replay_message = @connection.recovery_mode.full? ? message : nil
      register_sync_pending_confirm_replay_locked(replay_message, exchange, routing_key, mandatory)
    end

    private def register_sync_pending_confirm_replay_locked(replay_message : Message?,
                                                            exchange : String,
                                                            routing_key : String,
                                                            mandatory : Bool) : UInt64
      seq = @next_publish_seq
      @next_publish_seq += 1
      @unconfirmed << seq
      @lowest_unconfirmed ||= seq
      @pending_confirms[seq] = PendingConfirm.new(
        seq, mandatory, nil, nil, replay_message, exchange, routing_key, true,
      )
      seq
    end

    private def discard_pending_confirm_locked(tag : UInt64) : Nil
      @unconfirmed.delete(tag)
      @pending_confirms.delete(tag)
      @returned_confirms.delete(tag)
      discard_completed_sync_confirm_locked(tag)
      refresh_lowest_unconfirmed_locked if @lowest_unconfirmed == tag
    end

    private def take_completed_sync_confirm_locked(tag : UInt64) : ConfirmOutcome?
      if @completed_sync_confirm_tag == tag
        outcome = @completed_sync_confirm_outcome
        @completed_sync_confirm_tag = nil
        @completed_sync_confirm_outcome = nil
        return outcome
      end
      @completed_sync_confirms.delete(tag)
    end

    private def store_completed_sync_confirm_locked(tag : UInt64, outcome : ConfirmOutcome) : Nil
      if @completed_sync_confirm_tag.nil?
        @completed_sync_confirm_tag = tag
        @completed_sync_confirm_outcome = outcome
      else
        @completed_sync_confirms[tag] = outcome
      end
    end

    private def discard_completed_sync_confirm_locked(tag : UInt64) : Nil
      if @completed_sync_confirm_tag == tag
        @completed_sync_confirm_tag = nil
        @completed_sync_confirm_outcome = nil
      else
        @completed_sync_confirms.delete(tag)
      end
    end

    private def clear_completed_sync_confirms_locked : Nil
      @completed_sync_confirm_tag = nil
      @completed_sync_confirm_outcome = nil
      @completed_sync_confirms.clear
    end

    private def abandon_sync_confirm_waiter_locked(tag : UInt64) : Nil
      pending = @pending_confirms[tag]?
      return unless pending && pending.sync_waiter

      @pending_confirms[tag] = PendingConfirm.new(
        pending.original_tag, pending.mandatory, pending.outcome,
        pending.callback, pending.replay_message, pending.exchange, pending.routing_key, false,
      )
    end

    private def publish_batch_unconfirmed(messages : Array(Message),
                                          exchange : String,
                                          routing_key : String,
                                          mandatory : Bool,
                                          immediate : Bool) : Array(UInt64?)
      seqs = Array(UInt64?).new(messages.size) { nil }
      return seqs if messages.empty?

      max_body = max_body_per_frame(@connection.frame_max)
      empty_properties = all_properties_empty?(messages)
      method_frame = publish_method_frame(exchange, routing_key, mandatory, immediate)
      @connection.with_write do |io|
        if empty_properties
          messages.each do |message|
            write_publish_frames_to(io, method_frame, nil, message.body, max_body)
          end
        else
          messages.each do |message|
            header_payload = unless message.properties.empty?
              Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
                Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
                message.body.size.to_u64,
                message.properties,
              )
            end
            write_publish_frames_to(io, method_frame,
              header_payload, message.body, max_body)
          end
        end
      end
      @connection.stats.incr_published(messages.size.to_i64)
      seqs
    end

    private def all_properties_empty?(messages : Array(Message)) : Bool
      messages.each do |message|
        return false unless message.properties.empty?
      end
      true
    end

    private def all_properties_empty?(messages : Array(Message),
                                      start : Int32,
                                      stop : Int32) : Bool
      index = start
      while index < stop
        return false unless messages[index].properties.empty?
        index += 1
      end
      true
    end

    private def publish_batch_unconfirmed(bodies : Array(Bytes),
                                          exchange : String,
                                          routing_key : String,
                                          properties : Properties,
                                          mandatory : Bool,
                                          immediate : Bool) : Array(UInt64?)
      seqs = Array(UInt64?).new(bodies.size) { nil }
      return seqs if bodies.empty?

      max_body = max_body_per_frame(@connection.frame_max)
      method_frame = publish_method_frame(exchange, routing_key, mandatory, immediate)
      @connection.with_write do |io|
        if properties.empty?
          bodies.each do |body|
            write_publish_frames_to(io, method_frame, nil, body, max_body)
          end
        else
          bodies.each do |body|
            header_payload = Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
              Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
              body.size.to_u64,
              properties,
            )
            write_publish_frames_to(io, method_frame,
              header_payload, body, max_body)
          end
        end
      end
      @connection.stats.incr_published(bodies.size.to_i64)
      seqs
    end

    private def publish_batch_registered(messages : Array(Message),
                                         exchange : String,
                                         routing_key : String,
                                         mandatory : Bool,
                                         immediate : Bool) : Array(UInt64?)
      seqs = Array(UInt64?).new(messages.size)
      return seqs if messages.empty?

      max_body = max_body_per_frame(@connection.frame_max)
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      method_frame = publish_method_frame(exchange, routing_key, mandatory, immediate)
      empty_properties = all_properties_empty?(messages)
      @confirms_mutex.lock if lock_during_write
      begin
        @connection.with_write do |io|
          if lock_during_write
            messages.each do |message|
              seqs << register_pending_confirm_locked(message, exchange, routing_key, mandatory, nil)
            end
          else
            @confirms_mutex.synchronize do
              messages.each do |message|
                seqs << register_pending_confirm_locked(message, exchange, routing_key, mandatory, nil)
              end
            end
          end

          if empty_properties
            messages.each do |message|
              write_publish_frames_to(io, method_frame, nil, message.body, max_body)
            end
          else
            messages.each do |message|
              header_payload = unless message.properties.empty?
                Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
                  Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
                  message.body.size.to_u64,
                  message.properties,
                )
              end

              write_publish_frames_to(io, method_frame,
                header_payload, message.body, max_body)
            end
          end
        end
      rescue ex
        unless seqs.empty?
          if lock_during_write
            seqs.each { |tag| discard_pending_confirm_locked(tag) if tag }
          else
            @confirms_mutex.synchronize do
              seqs.each { |tag| discard_pending_confirm_locked(tag) if tag }
            end
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published(messages.size.to_i64)
      seqs
    end

    private def publish_batch_registered(bodies : Array(Bytes),
                                         exchange : String,
                                         routing_key : String,
                                         properties : Properties,
                                         mandatory : Bool,
                                         immediate : Bool) : Array(UInt64?)
      seqs = Array(UInt64?).new(bodies.size)
      return seqs if bodies.empty?

      max_body = max_body_per_frame(@connection.frame_max)
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      method_frame = publish_method_frame(exchange, routing_key, mandatory, immediate)
      @confirms_mutex.lock if lock_during_write
      begin
        @connection.with_write do |io|
          if lock_during_write
            bodies.each do |body|
              replay_message = Message.new(body, properties)
              seqs << register_pending_confirm_replay_locked(replay_message,
                exchange, routing_key, mandatory, nil)
            end
          else
            @confirms_mutex.synchronize do
              bodies.each do
                seqs << register_pending_confirm_replay_locked(nil,
                  exchange, routing_key, mandatory, nil)
              end
            end
          end

          if properties.empty?
            bodies.each do |body|
              write_publish_frames_to(io, method_frame, nil, body, max_body)
            end
          else
            bodies.each do |body|
              header_payload = Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
                Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
                body.size.to_u64,
                properties,
              )

              write_publish_frames_to(io, method_frame,
                header_payload, body, max_body)
            end
          end
        end
      rescue ex
        unless seqs.empty?
          if lock_during_write
            seqs.each { |tag| discard_pending_confirm_locked(tag) if tag }
          else
            @confirms_mutex.synchronize do
              seqs.each { |tag| discard_pending_confirm_locked(tag) if tag }
            end
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published(bodies.size.to_i64)
      seqs
    end

    private def publish_batch_registered_range(messages : Array(Message),
                                               start : Int32,
                                               count : Int32,
                                               exchange : String,
                                               routing_key : String,
                                               mandatory : Bool,
                                               immediate : Bool) : Nil
      return if count <= 0

      seqs = Array(UInt64).new(count)
      max_body = max_body_per_frame(@connection.frame_max)
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      method_frame = publish_method_frame(exchange, routing_key, mandatory, immediate)
      stop = start + count
      empty_properties = all_properties_empty?(messages, start, stop)

      @confirms_mutex.lock if lock_during_write
      begin
        @connection.with_write do |io|
          if lock_during_write
            index = start
            while index < stop
              message = messages[index]
              seqs << register_pending_confirm_locked(message, exchange, routing_key, mandatory, nil)
              index += 1
            end
          else
            @confirms_mutex.synchronize do
              index = start
              while index < stop
                message = messages[index]
                seqs << register_pending_confirm_locked(message, exchange, routing_key, mandatory, nil)
                index += 1
              end
            end
          end

          if empty_properties
            index = start
            while index < stop
              message = messages[index]
              write_publish_frames_to(io, method_frame, nil, message.body, max_body)
              index += 1
            end
          else
            index = start
            while index < stop
              message = messages[index]
              header_payload = unless message.properties.empty?
                Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
                  Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
                  message.body.size.to_u64,
                  message.properties,
                )
              end

              write_publish_frames_to(io, method_frame,
                header_payload, message.body, max_body)
              index += 1
            end
          end
        end
      rescue ex
        if lock_during_write
          seqs.each { |tag| discard_pending_confirm_locked(tag) }
        else
          @confirms_mutex.synchronize do
            seqs.each { |tag| discard_pending_confirm_locked(tag) }
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published(count.to_i64)
    end

    private def publish_batch_registered_range(bodies : Array(Bytes),
                                               start : Int32,
                                               count : Int32,
                                               exchange : String,
                                               routing_key : String,
                                               properties : Properties,
                                               mandatory : Bool,
                                               immediate : Bool) : Nil
      return if count <= 0

      seqs = Array(UInt64).new(count)
      max_body = max_body_per_frame(@connection.frame_max)
      lock_during_write = @confirms_enabled && @connection.recovery_mode.full?
      method_frame = publish_method_frame(exchange, routing_key, mandatory, immediate)
      stop = start + count

      @confirms_mutex.lock if lock_during_write
      begin
        @connection.with_write do |io|
          if lock_during_write
            index = start
            while index < stop
              body = bodies[index]
              replay_message = Message.new(body, properties)
              seqs << register_pending_confirm_replay_locked(replay_message,
                exchange, routing_key, mandatory, nil)
              index += 1
            end
          else
            @confirms_mutex.synchronize do
              index = start
              while index < stop
                seqs << register_pending_confirm_replay_locked(nil,
                  exchange, routing_key, mandatory, nil)
                index += 1
              end
            end
          end

          if properties.empty?
            index = start
            while index < stop
              body = bodies[index]
              write_publish_frames_to(io, method_frame, nil, body, max_body)
              index += 1
            end
          else
            index = start
            while index < stop
              body = bodies[index]
              header_payload = Amqp::Wire::AmqpZeroNineOne::ContentHeader.encode(
                Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC,
                body.size.to_u64,
                properties,
              )

              write_publish_frames_to(io, method_frame,
                header_payload, body, max_body)
              index += 1
            end
          end
        end
      rescue ex
        if lock_during_write
          seqs.each { |tag| discard_pending_confirm_locked(tag) }
        else
          @confirms_mutex.synchronize do
            seqs.each { |tag| discard_pending_confirm_locked(tag) }
          end
        end
        raise ex
      ensure
        @confirms_mutex.unlock if lock_during_write
      end
      @connection.stats.incr_published(count.to_i64)
    end

    private def publish_method_frame(exchange : String,
                                     routing_key : String,
                                     mandatory : Bool,
                                     immediate : Bool) : Bytes
      Amqp::Wire::AmqpZeroNineOne::BasicMethods.publish_frame(
        @id, exchange, routing_key, mandatory, immediate,
      )
    end

    private def cached_publish_method_frame_for(exchange : String,
                                                routing_key : String,
                                                mandatory : Bool,
                                                immediate : Bool) : Bytes?
      if frame = @cached_publish_method_frame
        if @cached_publish_method_frame_exchange == exchange &&
           @cached_publish_method_frame_routing_key == routing_key &&
           @cached_publish_method_frame_mandatory == mandatory &&
           @cached_publish_method_frame_immediate == immediate
          return frame
        end
      end

      if @publish_frame_candidate_exchange == exchange &&
         @publish_frame_candidate_routing_key == routing_key &&
         @publish_frame_candidate_mandatory == mandatory &&
         @publish_frame_candidate_immediate == immediate
        frame = publish_method_frame(exchange, routing_key, mandatory, immediate)
        @cached_publish_method_frame = frame
        @cached_publish_method_frame_exchange = exchange
        @cached_publish_method_frame_routing_key = routing_key
        @cached_publish_method_frame_mandatory = mandatory
        @cached_publish_method_frame_immediate = immediate
        return frame
      end

      @publish_frame_candidate_exchange = exchange
      @publish_frame_candidate_routing_key = routing_key
      @publish_frame_candidate_mandatory = mandatory
      @publish_frame_candidate_immediate = immediate
      nil
    end

    private def write_publish_frames(exchange : String,
                                     routing_key : String,
                                     mandatory : Bool,
                                     immediate : Bool,
                                     header_payload : Bytes?,
                                     body : Bytes,
                                     max_body : Int32,
                                     &before_write : ->) : Nil
      @connection.with_write do |io|
        before_write.call
        write_publish_frames_to(io, exchange, routing_key, mandatory, immediate,
          header_payload, body, max_body)
      end
    end

    private def write_publish_frames(method_frame : Bytes,
                                     header_payload : Bytes?,
                                     body : Bytes,
                                     max_body : Int32,
                                     &before_write : ->) : Nil
      @connection.with_write do |io|
        before_write.call
        write_publish_frames_to(io, method_frame, header_payload, body, max_body)
      end
    end

    private def write_prepared_publish_frames(prepared : PreparedPublisher,
                                              body : Bytes,
                                              max_body : Int32,
                                              &before_write : ->) : Nil
      @connection.with_write do |io|
        before_write.call
        write_prepared_publish_frames_to(io, prepared, body, max_body)
      end
    end

    private def write_publish_frames_to(io : IO,
                                        exchange : String,
                                        routing_key : String,
                                        mandatory : Bool,
                                        immediate : Bool,
                                        header_payload : Bytes?,
                                        body : Bytes,
                                        max_body : Int32) : Nil
      if method_frame = cached_publish_method_frame_for(exchange, routing_key, mandatory, immediate)
        io.write(method_frame)
      else
        Amqp::Wire::AmqpZeroNineOne::BasicMethods.write_publish_frame(
          io, @id, exchange, routing_key, mandatory, immediate,
        )
      end
      write_content_header_frame(io, body.size.to_u64, header_payload)
      offset = 0
      while offset < body.size
        chunk = Math.min(max_body, body.size - offset)
        write_body_frame(io, body, offset, chunk)
        offset += chunk
      end
    end

    private def write_publish_frames_to(io : IO,
                                        method_frame : Bytes,
                                        header_payload : Bytes?,
                                        body : Bytes,
                                        max_body : Int32) : Nil
      io.write(method_frame)
      write_content_header_frame(io, body.size.to_u64, header_payload)
      offset = 0
      while offset < body.size
        chunk = Math.min(max_body, body.size - offset)
        write_body_frame(io, body, offset, chunk)
        offset += chunk
      end
    end

    private def write_prepared_publish_frames_to(io : IO,
                                                 prepared : PreparedPublisher,
                                                 body : Bytes,
                                                 max_body : Int32) : Nil
      io.write(prepared.method_frame)
      if header_frame = prepared.__content_header_frame(body.size.to_u64)
        io.write(header_frame)
      elsif encoded = prepared.__encoded_properties
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.write_frame(
          io, @id, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC, body.size.to_u64, encoded,
        )
      else
        write_content_header_frame(io, body.size.to_u64, nil)
      end
      offset = 0
      while offset < body.size
        chunk = Math.min(max_body, body.size - offset)
        write_body_frame(io, body, offset, chunk)
        offset += chunk
      end
    end

    private def write_body_frame(io : IO, body : Bytes, offset : Int32, chunk : Int32) : Nil
      if prefix = cached_body_frame_prefix_for(chunk)
        io.write(prefix)
      else
        Amqp::Wire::Frame.write_prefix(io, Amqp::Wire::FrameType::Body, @id, chunk)
      end
      io.write(body[offset, chunk])
      io.write_byte(Amqp::Wire::FRAME_END)
    end

    private def write_content_header_frame(io : IO,
                                           body_size : UInt64,
                                           encoded_payload : Bytes?) : Nil
      if payload = encoded_payload
        Amqp::Wire::Frame.new(Amqp::Wire::FrameType::Header, @id, payload).write(io)
      elsif frame = cached_empty_content_header_frame_for(body_size)
        io.write(frame)
      else
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.write_empty_frame(
          io, @id, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC, body_size,
        )
      end
    end

    private def cached_empty_content_header_frame_for(body_size : UInt64) : Bytes?
      if frame = @cached_empty_content_header_frame
        return frame if @cached_empty_content_header_frame_body_size == body_size
      end

      if @empty_header_candidate_body_size == body_size
        frame = Amqp::Wire::AmqpZeroNineOne::ContentHeader.empty_frame(
          @id, Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC, body_size,
        )
        @cached_empty_content_header_frame = frame
        @cached_empty_content_header_frame_body_size = body_size
        return frame
      end

      @empty_header_candidate_body_size = body_size
      nil
    end

    private def cached_body_frame_prefix_for(chunk_size : Int32) : Bytes?
      if prefix = @cached_body_frame_prefix
        return prefix if @cached_body_frame_prefix_size == chunk_size
      end

      if @body_frame_prefix_candidate_size == chunk_size
        prefix = Amqp::Wire::Frame.prefix(Amqp::Wire::FrameType::Body, @id, chunk_size)
        @cached_body_frame_prefix = prefix
        @cached_body_frame_prefix_size = chunk_size
        return prefix
      end

      @body_frame_prefix_candidate_size = chunk_size
      nil
    end

    private def ensure_open! : Nil
      if @connection.state_recovering?
        raise RecoveryInProgress.new("connection is recovering")
      end
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
      enter_operation
      begin
        sync_rpc_locked(payload, default_rpc_timeout)
      ensure
        leave_operation
      end
    end

    private def sync_rpc_locked(payload : Bytes, timeout : Time::Span) : MethodEnvelope
      @sync_mutex.synchronize do
        if @connection.state_recovering?
          raise RecoveryInProgress.new("connection is recovering")
        end
        case @state
        when .recovering?
          raise RecoveryInProgress.new("channel #{@id} is recovering")
        when .closed?
          raise(@close_reason || ChannelClosedByCaller.new("channel #{@id} closed"))
        end
        slot = ::Channel(MethodEnvelope | Exception).new(1)
        @sync_slot = slot
        begin
          @connection.write_frame(@id, Amqp::Wire::FrameType::Method, payload)
        rescue ex
          @sync_slot = nil
          raise ex
        end

        reply = receive_sync_reply(slot, timeout)
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
      return if process_direct_confirm_frame(frame.payload)
      if deliver = decode_deliver_frame_payload_cached(frame.payload)
        @pending_method = deliver
        reset_pending_body_state
        return
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
        emit_close(cls.reply_code, cls.reply_text)
        finalize_closed(exc)
        return
      end

      if class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_CHANNEL &&
         method_id == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_CHANNEL_FLOW
        flow = Amqp::Wire::AmqpZeroNineOne::ChannelMethods::Flow.read(body)
        set_flow_active(flow.active)
        @connection.write_frame(@id, Amqp::Wire::FrameType::Method,
          Amqp::Wire::AmqpZeroNineOne::ChannelMethods::FlowOk.new(flow.active).to_payload)
        return
      end

      # Content-bearing methods → enter assembly mode.
      if class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC
        case method_id
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_DELIVER
          @pending_method = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver.read(body)
          reset_pending_body_state
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_RETURN
          @pending_method = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return.read(body)
          reset_pending_body_state
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_GET_OK
          @pending_method = Amqp::Wire::AmqpZeroNineOne::BasicMethods::GetOk.read(body)
          reset_pending_body_state
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_GET_EMPTY
          Amqp::Wire::AmqpZeroNineOne::BasicMethods::GetEmpty.read(body)
          slot = @get_slot
          if slot
            slot.send(nil)
          else
            raise ProtocolError.new("channel #{@id}: unsolicited basic.get-empty")
          end
          return
        when Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_CANCEL
          cancel = Amqp::Wire::AmqpZeroNineOne::BasicMethods::Cancel.read(body)
          sub = delete_consumer(cancel.consumer_tag)
          @topology_mutex.synchronize { @topology_consumers.delete(cancel.consumer_tag) }
          sub.try &.mark_closed
          unless cancel.no_wait
            @connection.write_frame(@id, Amqp::Wire::FrameType::Method,
              Amqp::Wire::AmqpZeroNineOne::BasicMethods::CancelOk.new(cancel.consumer_tag).to_payload)
          end
          emit_cancel(cancel.consumer_tag)
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

    private def reset_pending_body_state : Nil
      @pending_body.clear
      @pending_body_direct = nil
      @pending_body_received = 0_u64
    end

    private def register_consumer(tag : String, sub : Subscription) : Nil
      @consumers_mutex.synchronize do
        @consumers[tag] = sub
        @consumers_generation.add(1_i64)
      end
    end

    private def delete_consumer(tag : String) : Subscription?
      @consumers_mutex.synchronize do
        sub = @consumers.delete(tag)
        @consumers_generation.add(1_i64) if sub
        sub
      end
    end

    private def consumer_for(tag : String) : Subscription?
      generation = @consumers_generation.get
      if @cached_consumer_generation == generation &&
         @cached_consumer_tag == tag
        return @cached_consumer
      end

      @consumers_mutex.synchronize do
        sub = @consumers[tag]?
        @cached_consumer_tag = tag
        @cached_consumer = sub
        @cached_consumer_generation = @consumers_generation.get
        sub
      end
    end

    private def process_direct_confirm_frame(payload : Bytes) : Bool
      return false unless payload.size == 13
      return false unless read_u16_be(payload, 0) == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC

      method_id = read_u16_be(payload, 2)
      return false unless method_id == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_ACK ||
                          method_id == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_NACK

      tag = read_u64_be(payload, 4)
      bits = payload[12]
      settle_publish(tag, (bits & 0x01) != 0,
        nacked: method_id == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_NACK)
      true
    end

    private def read_u16_be(bytes : Bytes, offset : Int32) : UInt16
      ((bytes[offset].to_u16 << 8) | bytes[offset + 1].to_u16).to_u16
    end

    private def read_u64_be(bytes : Bytes, offset : Int32) : UInt64
      value = 0_u64
      8.times do |i|
        value = (value << 8) | bytes[offset + i].to_u64
      end
      value
    end

    private def decode_deliver_frame_payload_cached(payload : Bytes) : Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver?
      return nil unless payload.size >= 16
      return nil unless read_u16_be(payload, 0) == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC
      return nil unless read_u16_be(payload, 2) == Amqp::Wire::AmqpZeroNineOne::METHOD_ID_BASIC_DELIVER

      offset = 4
      tag_and_offset = read_shortstr_cached(payload, offset, @cached_deliver_consumer_tag) || return nil
      consumer_tag = tag_and_offset[0]
      offset = tag_and_offset[1]
      return nil if payload.size < offset + 9

      delivery_tag = read_u64_be(payload, offset)
      offset += 8
      redelivered = (payload[offset] & 0x01) != 0
      offset += 1

      exchange_and_offset = read_shortstr_cached(payload, offset, @cached_deliver_exchange) || return nil
      exchange = exchange_and_offset[0]
      offset = exchange_and_offset[1]
      routing_key_and_offset = read_shortstr_cached(payload, offset, @cached_deliver_routing_key) || return nil
      routing_key = routing_key_and_offset[0]
      offset = routing_key_and_offset[1]
      return nil unless offset == payload.size

      @cached_deliver_consumer_tag = consumer_tag
      @cached_deliver_exchange = exchange
      @cached_deliver_routing_key = routing_key

      Amqp::Wire::AmqpZeroNineOne::BasicMethods::Deliver.new(
        consumer_tag, delivery_tag, redelivered, exchange, routing_key,
      )
    end

    private def read_shortstr_cached(payload : Bytes,
                                     offset : Int32,
                                     cached : String?) : Tuple(String, Int32)?
      return nil if offset >= payload.size

      size = payload[offset].to_i32
      start = offset + 1
      finish = start + size
      return nil if payload.size < finish

      if cached && shortstr_matches?(payload, start, size, cached)
        {cached, finish}
      else
        {String.new(payload[start, size]), finish}
      end
    end

    private def shortstr_matches?(payload : Bytes,
                                  start : Int32,
                                  size : Int32,
                                  cached : String) : Bool
      cached_bytes = cached.to_slice
      return false unless cached_bytes.size == size

      index = 0
      while index < size
        return false unless payload[start + index] == cached_bytes[index]
        index += 1
      end
      true
    end

    private def process_header_frame(frame : Amqp::Wire::Frame) : Nil
      pending = @pending_method
      raise ProtocolError.new("channel #{@id}: header without method") if pending.nil?
      if metadata = Amqp::Wire::AmqpZeroNineOne::ContentHeader.decode_empty_metadata(frame.payload)
        class_id = metadata[0]
        body_size = metadata[1]
        properties = nil
      else
        decoded = Amqp::Wire::AmqpZeroNineOne::ContentHeader.decode(frame.payload)
        class_id = decoded.class_id
        body_size = decoded.body_size
        properties = decoded.properties
      end

      unless class_id == Amqp::Wire::AmqpZeroNineOne::CLASS_ID_BASIC
        raise ProtocolError.new("channel #{@id}: header class #{class_id} != 60")
      end
      @pending_props = properties
      @pending_body_size = body_size
      @pending_body_received = 0_u64
      if @pending_body_size == 0
        emit_pending_delivery
      end
    end

    private def process_body_frame(frame : Amqp::Wire::Frame) : Nil
      raise ProtocolError.new("channel #{@id}: body without header") if @pending_method.nil?
      if @pending_body_received == 0 && frame.payload.size.to_u64 == @pending_body_size
        @pending_body_direct = frame.payload
      else
        @pending_body.write(frame.payload)
      end
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
      props = @pending_props || EMPTY_PROPERTIES
      body_direct = @pending_body_direct
      body_bytes = body_direct || @pending_body.to_slice
      @pending_method = nil
      @pending_props = nil
      @pending_body_size = 0_u64
      @pending_body_received = 0_u64
      @pending_body_direct = nil
      if body_direct || @pending_body.empty?
        @pending_body.clear
      else
        @pending_body = IO::Memory.new
      end

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
        sub = consumer_for(method.consumer_tag)
        sub.try &.deliver(delivery)
        @connection.stats.incr_consumed
      in Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return
        emit_return(method, props, body_bytes)
        @connection.stats.incr_returned
      in Amqp::Wire::AmqpZeroNineOne::BasicMethods::GetOk
        msg = GetMessage.new(
          body: body_bytes,
          properties: props,
          delivery_tag: method.delivery_tag,
          redelivered: method.redelivered,
          exchange: method.exchange,
          routing_key: method.routing_key,
          message_count: method.message_count,
          channel: self,
        )
        slot = @get_slot
        if slot
          slot.send(msg)
        else
          raise ProtocolError.new("channel #{@id}: unsolicited basic.get-ok content")
        end
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
      get_slot = @get_slot
      get_slot.try &.send(exc)
    end

    private def record_return(ret : Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return) : Nil
      reason = ReturnReason.new(ret.reply_code, ret.reply_text, ret.exchange, ret.routing_key)
      @confirms_mutex.synchronize do
        tag = @pending_confirms
          .select { |_seq, pending| pending.mandatory }
          .keys
          .min?
        @returned_confirms[tag] = reason if tag
      end
    end

    private def emit_return(ret : Amqp::Wire::AmqpZeroNineOne::BasicMethods::Return,
                            properties : Properties,
                            body : Bytes) : Nil
      record_return(ret)
      if callback = @on_return
        returned = ReturnedMessage.new(
          ret.reply_code,
          ret.reply_text,
          ret.exchange,
          ret.routing_key,
          properties,
          body,
        )
        spawn(name: "amqp-return-#{@id}") { callback.call(returned) }
      end
    end

    private def emit_cancel(consumer_tag : String) : Nil
      if callback = @on_cancel
        spawn(name: "amqp-cancel-#{@id}") { callback.call(consumer_tag) }
      end
    end

    private def emit_close(reply_code : UInt16, reply_text : String) : Nil
      if callback = @on_close
        spawn(name: "amqp-close-#{@id}") { callback.call(reply_code, reply_text) }
      end
    end

    # Settle a single seq (multiple=false) or all seqs up to and including
    # `tag` (multiple=true). Sets nack-flag if any settled seq was negative,
    # then wakes any wait_for_confirms waiters.
    private def settle_publish(tag : UInt64, multiple : Bool, nacked : Bool) : Nil
      single_settled = nil
      multiple_settled = nil
      @confirms_mutex.synchronize do
        if multiple
          raise PublishOutOfOrderError.new(tag) unless @unconfirmed.includes?(tag)
          settled = settle_multiple_publishes_locked(tag, nacked)
          multiple_settled = settled
          @confirms_nacked = true if nacked || settled.any? { |entry| entry.outcome.kind.returned? }
        else
          unless @pending_confirms.has_key?(tag)
            raise PublishOutOfOrderError.new(tag)
          end
          settled = settle_one_publish_locked(tag, nacked)
          single_settled = settled
          @confirms_nacked = true if nacked || settled.outcome.kind.returned?
        end
        wake_confirms_locked
      end

      if settled = single_settled
        outcome = settled.outcome
        if outcome.kind.nack?
          @connection.stats.incr_confirmed_nack
        else
          @connection.stats.incr_confirmed_ack
        end
        settled.callback.try { |callback| enqueue_confirm_callback(callback, outcome.kind.ack?) }
      elsif settled_entries = multiple_settled
        ack_count = 0_i64
        nack_count = 0_i64
        settled_entries.each do |settled|
          outcome = settled.outcome
          if outcome.kind.nack?
            nack_count += 1
          else
            ack_count += 1
          end
          settled.callback.try { |callback| enqueue_confirm_callback(callback, outcome.kind.ack?) }
        end
        @connection.stats.incr_confirmed_ack(ack_count) if ack_count > 0
        @connection.stats.incr_confirmed_nack(nack_count) if nack_count > 0
      end
    end

    private def settle_multiple_publishes_locked(tag : UInt64, nacked : Bool) : Array(SettledPublish)
      low = @lowest_unconfirmed || tag
      span = tag - low + 1_u64
      dense_limit = @unconfirmed.size.to_u64 * 4_u64
      settled = [] of SettledPublish

      if span <= dense_limit
        seq = low
        loop do
          if @unconfirmed.includes?(seq)
            settled << settle_one_publish_locked(seq, nacked, refresh_lowest: false)
          end
          break if seq == tag
          seq += 1_u64
        end
      else
        tags = @unconfirmed.select { |seq| seq <= tag }.sort
        tags.each do |seq|
          settled << settle_one_publish_locked(seq, nacked, refresh_lowest: false)
        end
      end

      refresh_lowest_unconfirmed_locked
      settled
    end

    private def enqueue_confirm_callback(callback : ConfirmCallback, ok : Bool) : Nil
      select
      when @confirm_callback_queue.send({callback, ok})
      else
        spawn(name: "amqp-confirm-callback-overflow-#{@id}") do
          call_confirm_callback(callback, ok)
        end
      end
    rescue ::Channel::ClosedError
      call_confirm_callback(callback, ok)
    end

    private def run_confirm_callback_loop : Nil
      while entry = @confirm_callback_queue.receive?
        callback, ok = entry
        call_confirm_callback(callback, ok)
      end
    end

    private def call_confirm_callback(callback : ConfirmCallback, ok : Bool) : Nil
      callback.call(ok)
    rescue ex
      Log.warn(exception: ex) { "basic_publish confirm callback failed" }
    end

    private def settle_one_publish_locked(tag : UInt64,
                                          nacked : Bool,
                                          *,
                                          refresh_lowest : Bool = true) : SettledPublish
      @unconfirmed.delete(tag)
      if refresh_lowest && @lowest_unconfirmed == tag
        if @unconfirmed.empty?
          @lowest_unconfirmed = nil
        else
          refresh_lowest_unconfirmed_locked
        end
      end
      pending = @pending_confirms.delete(tag)
      reason = @returned_confirms.delete(tag)
      kind = if reason
               ConfirmOutcome::Kind::Returned
             elsif nacked
               ConfirmOutcome::Kind::Nack
             else
               ConfirmOutcome::Kind::Ack
             end
      outcome = ConfirmOutcome.new(kind, tag, reason)
      if pending && (ch = pending.outcome)
        begin
          ch.send(outcome)
        rescue
        ensure
          ch.close rescue nil
        end
      elsif pending && pending.sync_waiter
        store_completed_sync_confirm_locked(tag, outcome)
      end
      SettledPublish.new(outcome, pending.try &.callback)
    end

    # Caller MUST already hold @confirms_mutex.
    private def refresh_lowest_unconfirmed_locked : Nil
      @lowest_unconfirmed = nil
      @unconfirmed.each do |seq|
        low = @lowest_unconfirmed
        @lowest_unconfirmed = seq if low.nil? || seq < low
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
      callbacks = [] of ConfirmCallback
      @consumers_mutex.synchronize do
        @consumers.each_value(&.mark_closed)
        @consumers.clear
      end
      @confirms_mutex.synchronize do
        @pending_confirms.each_value do |pending|
          pending.outcome.try { |ch| ch.close rescue nil }
          pending.callback.try { |callback| callbacks << callback }
        end
        @pending_confirms.clear
        @unconfirmed.clear
        @lowest_unconfirmed = nil
        @returned_confirms.clear
        clear_completed_sync_confirms_locked
        wake_confirms_locked
      end
      @flow_mutex.synchronize do
        old = @flow_waker
        @flow_waker = ::Channel(Nil).new
        old.close rescue nil
      end
      @connection.unregister_channel(@id)
      @inbox.close rescue nil
      @confirm_callback_queue.close rescue nil
      callbacks.each { |callback| call_confirm_callback(callback, false) }
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

      @confirms_mutex.synchronize do
        @unconfirmed.clear
        @lowest_unconfirmed = nil
        @returned_confirms.clear
        clear_completed_sync_confirms_locked
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
        @confirms_mutex.synchronize do
          @pending_confirms.each do |seq, pending|
            new_routing_key = renames[pending.routing_key]?
            next unless pending.exchange.empty? && new_routing_key
            @pending_confirms[seq] = PendingConfirm.new(
              pending.original_tag, pending.mandatory, pending.outcome,
              pending.callback, pending.replay_message, pending.exchange, new_routing_key,
              pending.sync_waiter,
            )
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

      republish_unconfirmed

      @confirms_mutex.synchronize { @confirms_nacked = false }

      @state = State::Open
    end

    private def republish_unconfirmed : Nil
      pending = @confirms_mutex.synchronize do
        values = @pending_confirms.values.sort_by(&.original_tag)
        @pending_confirms.clear
        @unconfirmed.clear
        @lowest_unconfirmed = nil
        @returned_confirms.clear
        @next_publish_seq = 1_u64
        values
      end
      pending.each do |entry|
        publish_registered(entry.message_for_replay, entry.exchange, entry.routing_key,
          entry.mandatory, false, entry.outcome)
      end
    end

    # Test-only hook for confirm tracker invariants without a synthetic broker.
    private def __settle_publish_for_test(tag : UInt64, multiple : Bool, nacked : Bool) : Nil
      settle_publish(tag, multiple, nacked)
    end

    private def __await_publish_confirm_for_test(tag : UInt64,
                                                 outcome : ::Channel(ConfirmOutcome),
                                                 timeout : Time::Span) : Bool
      select
      when result = outcome.receive?
        raise ChannelClosedByCaller.new("confirm channel closed before outcome") if result.nil?
        handle_publish_confirm_result(result, "", "spec")
      when timeout(timeout)
        raise PublishTimeoutError.new(tag, timeout)
      end
    end
  end
end
