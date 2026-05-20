require "json"

path = ARGV[0]? || "-"
raw = path == "-" ? STDIN.gets_to_end : File.read(path)
doc = JSON.parse(raw)

errors = [] of String

def env_float(name : String, fallback : Float64) : Float64
  value = ENV[name]?
  return fallback unless value
  parsed = value.to_f64?
  raise "#{name} must be a float" unless parsed
  parsed
end

def env_list(name : String, fallback : String) : Array(String)
  (ENV[name]? || fallback)
    .split(',')
    .map(&.strip)
    .reject(&.empty?)
end

def object_field(root : JSON::Any, name : String, errors : Array(String)) : Hash(String, JSON::Any)?
  value = root[name]?
  unless value
    errors << "missing #{name}"
    return nil
  end
  value.as_h?
rescue
  errors << "#{name} is not an object"
  nil
end

def float_field(root : JSON::Any, name : String, errors : Array(String), context : String) : Float64?
  value = root[name]?
  unless value
    errors << "#{context}: missing #{name}"
    return nil
  end
  value.as_f
rescue
  errors << "#{context}: #{name} is not a number"
  nil
end

def samples_field(root : JSON::Any, errors : Array(String), context : String) : Array(JSON::Any)?
  value = root["samples"]?
  unless value
    errors << "#{context}: missing samples"
    return nil
  end
  value.as_a?
rescue
  errors << "#{context}: samples is not an array"
  nil
end

min_metric_median = env_float("AMQP_BENCH_MIN_METRIC_MEDIAN", 1.0)
min_stage_median = env_float("AMQP_BENCH_MIN_STAGE_MEDIAN", 1.0)
required_metrics = env_list("AMQP_BENCH_REQUIRED_METRICS", "publish_single,confirm_sync")
required_stages = env_list("AMQP_BENCH_REQUIRED_STAGES", "encode_empty_publish_frames")

tool = doc["tool"]?.try(&.as_s?)
errors << "unexpected tool #{tool.inspect}" unless tool == "amqp-ng publish microbench"

metrics = object_field(doc, "metrics", errors)
stages = object_field(doc, "stages", errors)

if metrics
  if metrics.empty?
    errors << "metrics is empty"
  end

  required_metrics.each do |name|
    errors << "missing metric #{name}" unless metrics.has_key?(name)
  end

  metrics.each do |name, entry|
    median = float_field(entry, "median", errors, "metric #{name}")
    if median && median < min_metric_median
      errors << "metric #{name}: median #{median} below #{min_metric_median}"
    end

    samples = samples_field(entry, errors, "metric #{name}")
    if samples
      errors << "metric #{name}: samples is empty" if samples.empty?
      samples.each_with_index do |sample, index|
        value = sample.as_f
        errors << "metric #{name}: sample #{index} is below #{min_metric_median}" if value < min_metric_median
      rescue
        errors << "metric #{name}: sample #{index} is not a number"
      end
    end
  end
end

if stages
  if stages.empty?
    errors << "stages is empty"
  end

  required_stages.each do |name|
    errors << "missing stage #{name}" unless stages.has_key?(name)
  end

  stages.each do |name, entry|
    median = float_field(entry, "median", errors, "stage #{name}")
    if median && median < min_stage_median
      errors << "stage #{name}: median #{median} below #{min_stage_median}"
    end

    samples = samples_field(entry, errors, "stage #{name}")
    if samples
      errors << "stage #{name}: samples is empty" if samples.empty?
      samples.each_with_index do |sample, index|
        value = sample.as_f
        errors << "stage #{name}: sample #{index} is below #{min_stage_median}" if value < min_stage_median
      rescue
        errors << "stage #{name}: sample #{index} is not a number"
      end
    end
  end
end

unless errors.empty?
  STDERR.puts "perf smoke assertion failed:"
  errors.each { |error| STDERR.puts "- #{error}" }
  exit 1
end

puts "perf smoke assertion passed: #{metrics.try(&.size) || 0} metrics, #{stages.try(&.size) || 0} stages"
