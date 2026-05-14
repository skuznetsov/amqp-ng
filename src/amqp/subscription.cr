require "./delivery"

module Amqp
  # A live consumer registration. Iterate over `receive` to consume.
  # When the broker cancels the consumer (e.g., queue deleted) or the
  # channel closes, `receive` raises `SubscriptionClosed`.
  class Subscription
    class Closed < Error
    end

    getter consumer_tag : String
    @channel : Channel
    @mailbox : ::Channel(Delivery)
    @closed : Bool

    protected def initialize(@channel : Channel, @consumer_tag : String)
      @mailbox = ::Channel(Delivery).new(1024)
      @closed = false
    end

    protected def deliver(d : Delivery) : Nil
      return if @closed
      @mailbox.send(d)
    end

    def receive : Delivery
      raise Closed.new("subscription #{@consumer_tag} closed") if @closed && @mailbox.empty?
      msg = @mailbox.receive?
      raise Closed.new("subscription #{@consumer_tag} closed") if msg.nil?
      msg
    end

    def receive? : Delivery?
      return nil if @closed && @mailbox.empty?
      @mailbox.receive?
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
      @mailbox = ::Channel(Delivery).new(1024)
      old.close rescue nil
    end

    def closed? : Bool
      @closed
    end

    def close : Nil
      return if @closed
      @channel.cancel(@consumer_tag)
    end
  end
end
