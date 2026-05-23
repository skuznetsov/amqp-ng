require "./perf_spec_helper"

private def perf_recovery_container : String?
  ENV["AMQP_CHAOS_DOCKER_CONTAINER"]?
end

private def perf_docker_available? : Bool
  Process.run("docker", ["--version"], output: Process::Redirect::Close,
    error: Process::Redirect::Close).success?
rescue
  false
end

private def perf_docker(*args : String) : Nil
  status = Process.run("docker", args.to_a)
  raise "docker #{args.join(' ')} failed" unless status.success?
end

private def wait_for_recovered_publish_confirm(conn : Amqp::Connection,
                                               ch : Amqp::Channel,
                                               queue : String,
                                               timeout_span : Time::Span) : Nil
  deadline = Time.instant + timeout_span
  loop do
    if conn.recovered_to_open?
      ch.publish_confirm("post-recovery".to_slice, "", queue, timeout: 5.seconds).should be_true
      return
    end

    raise "connection did not recover within #{timeout_span.total_seconds}s" if Time.instant > deadline
    sleep 50.milliseconds
  end
end

describe "T-PERF-RECOV-001" do
  it "measures broker-restart recovery dead window until first confirmed publish" do
    pending! "set AMQP_PERF_LIVE=1 to run live perf specs" unless PerfSpecHelper.live_enabled?
    container = perf_recovery_container
    pending! "set AMQP_CHAOS_DOCKER_CONTAINER to run broker-restart recovery perf" unless container
    pending! "docker unavailable" unless perf_docker_available?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    samples = PerfSpecHelper.int_env("AMQP_PERF_RECOV_SAMPLES", 5)
    bound = PerfSpecHelper.float_env("AMQP_PERF_RECOV_001_MAX_MS", 2_000.0)
    timeout_span = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_RECOV_TIMEOUT_SECONDS", 30.0)
    )
    initial_delay = PerfSpecHelper.span(
      PerfSpecHelper.float_env("AMQP_PERF_RECOV_INITIAL_DELAY_SECONDS", 0.1)
    )
    measurements = [] of Float64

    samples.times do |index|
      Amqp.connect(SpecHelper.amqp_url, recovery: true,
        recovery_initial_delay: initial_delay,
        recovery_max_attempts: PerfSpecHelper.int_env("AMQP_PERF_RECOV_MAX_ATTEMPTS", 30)) do |conn|
        ch = conn.channel
        ch.confirm_select
        queue = "amqp-ng-perf-recov-#{Process.pid}-#{Time.utc.to_unix_ms}-#{index}"
        info = ch.queue_declare(queue, durable: false, exclusive: false, auto_delete: true)

        started = Time.instant
        perf_docker("restart", container)
        wait_for_recovered_publish_confirm(conn, ch, info.name, timeout_span)
        measurements << (Time.instant - started).total_milliseconds
      end
    end

    median = PerfSpecHelper.percentile(measurements, 0.50)
    passed = median <= bound
    metadata = {
      "sample_count"                   => JSON::Any.new(samples.to_i64),
      "min_ms"                         => JSON::Any.new(measurements.min),
      "max_ms"                         => JSON::Any.new(measurements.max),
      "recovery_initial_delay_seconds" => JSON::Any.new(initial_delay.total_seconds),
      "timeout_seconds"                => JSON::Any.new(timeout_span.total_seconds),
      "amqp_url"                       => JSON::Any.new(PerfSpecHelper.redacted_url(SpecHelper.amqp_url)),
    }
    PerfSpecHelper.write_result(
      "T-PERF-RECOV-001", "recovery_dead_window_median", median, bound, "ms", passed, metadata
    )

    median.should be <= bound
  end
end
