require "./delivery"

module Amqp
  # A live consumer registration. Iterate over `receive` to consume.
  # When the broker cancels the consumer (e.g., queue deleted) or the
  # channel closes, `receive` raises `SubscriptionClosed`.
  class Subscription
    class Closed < Error
    end

    getter consumer_tag : String
    getter channel : Channel
    getter queue : String
    @channel : Channel
    @mailbox : ::Channel(Delivery)
    @capacity : Int32
    @closed : Bool

    protected def initialize(@channel : Channel, @consumer_tag : String, @queue : String = "", capacity : Int32 = 16)
      @capacity = capacity
      @mailbox = ::Channel(Delivery).new(capacity)
      @closed = false
    end

    protected def deliver(d : Delivery) : Nil
      return if @closed
      @mailbox.send(d)
    end

    def receive : Delivery
      msg = @mailbox.receive?
      raise Closed.new("subscription #{@consumer_tag} closed") if msg.nil?
      msg
    end

    def receive? : Delivery?
      @mailbox.receive?
    end

    # Select support for `select; when msg = sub.receive; ...; end`.
    def receive_select_action
      @mailbox.receive_select_action
    end

    def receive_select_action?
      @mailbox.receive_select_action?
    end

    protected def mark_closed : Nil
      @closed = true
      @mailbox.close
    end

    # Drop everything buffered in the mailbox. Used by Channel during
    # recovery — old deliveries have stale per-session delivery_tags that
    # the broker on the new session won't recognize.
    protected def reset_mailbox : Nil
      old = @mailbox
      @mailbox = ::Channel(Delivery).new(@capacity)
      old.close rescue nil
    end

    def closed? : Bool
      @closed
    end

    def close : Nil
      return if @closed
      @channel.cancel(@consumer_tag)
    end

    def each(& : Delivery -> _) : Nil
      loop do
        yield receive
      rescue Closed
        break
      end
    end

    def spawn_loop(& : Delivery -> _) : Nil
      spawn(name: "amqp-subscription-#{@consumer_tag}") do
        each { |delivery| yield delivery }
      end
    end
  end

  alias SubscriptionClosed = Subscription::Closed
end
