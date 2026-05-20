require "./perf_spec_helper"

describe "T-PERF-PUB-002" do
  it "measures async publish-confirm throughput over a live broker window" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    warmup = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_WARMUP_SECONDS", PerfSpecHelper::DEFAULT_WARMUP)
    )
    window = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_WINDOW_SECONDS", PerfSpecHelper::DEFAULT_WINDOW)
    )
    body_bytes = PerfSpecHelper.int_env("AMQP_PERF_BODY_BYTES", 256)
    outcome_buffer = PerfSpecHelper.int_env("AMQP_PERF_CONFIRM_OUTCOME_BUFFER", 1024)
    bound = PerfSpecHelper.float_env("AMQP_PERF_PUB_002_MIN", 1.0)
    body = Bytes.new(body_bytes, 120_u8)
    message = Amqp::Message.new(body)

    count = 0
    elapsed = Time::Span.zero
    failures = 0

    Amqp.connect(SpecHelper.amqp_url, recovery: Amqp::Recovery::None) do |conn|
      ch = conn.channel
      queue = ch.queue_declare("", exclusive: true, auto_delete: true).name
      ch.confirm_select

      warmup_deadline = Time.instant + warmup
      while Time.instant < warmup_deadline
        ch.publish(message, "", queue)
      end
      ch.wait_for_confirms(30.seconds).should be_true
      ch.queue_purge(queue)

      outcomes = ::Channel(::Channel(Amqp::ConfirmOutcome)).new(outcome_buffer)
      drained = ::Channel(Int32).new(1)
      spawn(name: "amqp-perf-confirm-drain") do
        local_failures = 0
        while outcome_ch = outcomes.receive?
          outcome = outcome_ch.receive
          local_failures += 1 unless outcome.kind.ack?
        end
        drained.send(local_failures)
      end

      started = Time.instant
      deadline = started + window
      while Time.instant < deadline
        _tag, outcome_ch = ch.publish_async(message, "", queue)
        outcomes.send(outcome_ch)
        count += 1
      end
      outcomes.close
      failures = drained.receive
      elapsed = Time.instant - started
      ch.queue_purge(queue)
    end

    rate = count.to_f64 / elapsed.total_seconds
    passed = failures == 0 && rate >= bound
    metadata = {
      "warmup_seconds" => JSON::Any.new(warmup.total_seconds),
      "window_seconds" => JSON::Any.new(window.total_seconds),
      "body_bytes"     => JSON::Any.new(body_bytes.to_i64),
      "outcome_buffer" => JSON::Any.new(outcome_buffer.to_i64),
      "amqp_url"       => JSON::Any.new(SpecHelper.amqp_url),
    }
    PerfSpecHelper.write_result(
      "T-PERF-PUB-002", "publish_async_confirm", rate, bound, "msg/s", passed, metadata
    )

    failures.should eq(0)
    rate.should be >= bound
  end
end
