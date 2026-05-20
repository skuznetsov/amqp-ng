require "../spec_helper"
require "file_utils"
require "json"

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
