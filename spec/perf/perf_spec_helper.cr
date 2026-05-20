require "../spec_helper"
require "file_utils"
require "json"
require "uri"

module PerfSpecHelper
  extend self

  LIVE_ENV       = "AMQP_PERF_LIVE"
  RESULTS_DIR    = "spec/perf/results"
  DEFAULT_WINDOW = 1.0
  DEFAULT_WARMUP = 0.2

  def live_enabled? : Bool
    ENV[LIVE_ENV]? == "1"
  end

  def float_env(name : String, default : Float64) : Float64
    value = ENV[name]?
    return default unless value

    parsed = value.to_f64
    raise "#{name} must be positive" unless parsed > 0.0
    parsed
  end

  def int_env(name : String, default : Int32) : Int32
    value = ENV[name]?
    return default unless value

    parsed = value.to_i
    raise "#{name} must be positive" unless parsed > 0
    parsed
  end

  def span(seconds : Float64) : Time::Span
    Time::Span.new(nanoseconds: (seconds * 1_000_000_000).round.to_i64)
  end

  def legal_subscription_buffer(count : Int32, config : Amqp::Config) : Int32
    max_by_budget = config.max_subscription_mailbox_bytes // config.max_body_size
    cap = {max_by_budget, Int32::MAX.to_u64}.min.to_i
    {1, {count, 8192, cap}.min}.max
  end

  def percentile(values : Array(Float64), percentile : Float64) : Float64
    raise "percentile input must not be empty" if values.empty?
    raise "percentile must be in (0, 1]" unless percentile > 0.0 && percentile <= 1.0

    sorted = values.sort
    index = (percentile * sorted.size).ceil.to_i - 1
    index = 0 if index < 0
    index = sorted.size - 1 if index >= sorted.size
    sorted[index]
  end

  def measure_connect_ms(url : String,
                         count : Int32,
                         tls_context : OpenSSL::SSL::Context::Client? = nil) : Array(Float64)
    samples = [] of Float64
    count.times do
      started = Time.instant
      Amqp.connect(url, tls_context: tls_context, recovery: Amqp::Recovery::None,
        heartbeat: 0.seconds) do |_conn|
        samples << (Time.instant - started).total_milliseconds
      end
    end
    samples
  end

  def tls_context_from_env : OpenSSL::SSL::Context::Client?
    ca_cert = ENV["AMQP_TLS_CA_CERT"]?
    return nil unless ca_cert

    ctx = Amqp.tls_context_default
    ctx.ca_certificates = ca_cert
    ctx
  end

  def reachable?(url : String, tls_context : OpenSSL::SSL::Context::Client? = nil) : Bool
    conn = Amqp.connect(url, tls_context: tls_context, recovery: Amqp::Recovery::None,
      heartbeat: 0.seconds, connect_timeout: 0.5.seconds)
    conn.close
    true
  rescue
    false
  end

  def redacted_url(raw_url : String) : String
    uri = URI.parse(raw_url)
    userinfo = uri.user ? "#{uri.user}:<redacted>@" : ""
    host = uri.host || "<host>"
    port = uri.port ? ":#{uri.port}" : ""
    path = uri.path.presence || "/"
    query = uri.query ? "?..." : ""
    "#{uri.scheme}://#{userinfo}#{host}#{port}#{path}#{query}"
  rescue
    "<unparseable>"
  end

  def command_output(command : String, args : Array(String)) : String
    output = IO::Memory.new
    status = Process.run(command, args, output: output, error: Process::Redirect::Close)
    return output.to_s.strip if status.success?

    "unavailable"
  rescue
    "unavailable"
  end

  def host_fingerprint : Hash(String, String)
    cpu_probe = "sysctl -n machdep.cpu.brand_string 2>/dev/null || " \
                "grep -m1 'model name' /proc/cpuinfo 2>/dev/null"
    {
      "os"      => command_output("uname", ["-a"]),
      "cpu"     => command_output("sh", ["-c", cpu_probe]),
      "crystal" => {{ Crystal::DESCRIPTION.stringify }},
    }
  end

  def write_result(id : String,
                   metric : String,
                   value : Float64,
                   bound : Float64,
                   unit : String,
                   passed : Bool,
                   metadata : Hash(String, JSON::Any)) : Nil
    FileUtils.mkdir_p(RESULTS_DIR)
    now = Time.utc.to_rfc3339

    File.write("#{RESULTS_DIR}/#{id}.json", JSON.build(indent: "  ") do |json|
      json.object do
        json.field "id", id
        json.field "metric", metric
        json.field "value", value
        json.field "bound", bound
        json.field "unit", unit
        json.field "passed", passed
        json.field "timestamp", now
        json.field "host", host_fingerprint
        json.field "metadata", metadata
      end
    end)

    File.write("#{RESULTS_DIR}/#{id}.txt", <<-TEXT)
    #{id}
    metric: #{metric}
    value: #{value} #{unit}
    bound: #{bound} #{unit}
    pass: #{passed}
    timestamp: #{now}
    crystal: #{Crystal::DESCRIPTION}
    TEXT
  end
end
