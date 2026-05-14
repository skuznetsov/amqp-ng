module Amqp
  struct QueueInfo
    getter name : String
    getter message_count : UInt32
    getter consumer_count : UInt32

    def initialize(@name, @message_count, @consumer_count)
    end
  end
end
