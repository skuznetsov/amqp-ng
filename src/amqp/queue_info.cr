module Amqp
  struct QueueInfo
    getter name : String
    getter message_count : UInt32
    getter consumer_count : UInt32

    def initialize(@name, @message_count, @consumer_count)
    end

    def queue_name : String
      @name
    end

    def [](key : Symbol) : String | UInt32
      case key
      when :queue_name, :name
        @name
      when :message_count
        @message_count
      when :consumer_count
        @consumer_count
      else
        raise KeyError.new("Missing queue info key: #{key.inspect}")
      end
    end

    def [](key : String) : String | UInt32
      self[key.to_sym]
    end
  end
end
