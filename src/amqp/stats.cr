module Amqp
  # Atomic counters covering publish/consume volume and recovery activity.
  # Mutated from any fiber via the `incr_*` helpers; read as a snapshot
  # via `Connection#stats`.
  class Stats
    @published : Atomic(Int64)
    @confirmed_ack : Atomic(Int64)
    @confirmed_nack : Atomic(Int64)
    @returned : Atomic(Int64)
    @consumed : Atomic(Int64)
    @recoveries_attempted : Atomic(Int64)
    @recoveries_succeeded : Atomic(Int64)
    @recoveries_failed : Atomic(Int64)

    def initialize
      @published = Atomic(Int64).new(0_i64)
      @confirmed_ack = Atomic(Int64).new(0_i64)
      @confirmed_nack = Atomic(Int64).new(0_i64)
      @returned = Atomic(Int64).new(0_i64)
      @consumed = Atomic(Int64).new(0_i64)
      @recoveries_attempted = Atomic(Int64).new(0_i64)
      @recoveries_succeeded = Atomic(Int64).new(0_i64)
      @recoveries_failed = Atomic(Int64).new(0_i64)
    end

    {% for name in %w(published confirmed_ack confirmed_nack returned consumed
                     recoveries_attempted recoveries_succeeded recoveries_failed) %}
      def {{name.id}} : Int64
        @{{name.id}}.get
      end

      protected def incr_{{name.id}}(by : Int64 = 1_i64) : Nil
        @{{name.id}}.add(by)
      end
    {% end %}

    # Immutable snapshot suitable for logging or reporting.
    record Snapshot,
      published : Int64,
      confirmed_ack : Int64,
      confirmed_nack : Int64,
      returned : Int64,
      consumed : Int64,
      recoveries_attempted : Int64,
      recoveries_succeeded : Int64,
      recoveries_failed : Int64

    def snapshot : Snapshot
      Snapshot.new(
        published, confirmed_ack, confirmed_nack, returned, consumed,
        recoveries_attempted, recoveries_succeeded, recoveries_failed,
      )
    end
  end
end
