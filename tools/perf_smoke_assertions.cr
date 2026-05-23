require "json"

module AmqpPerfSmokeAssert
  DEFAULT_REQUIRED_METRICS = %w(
    publish_single
    confirm_sync
    confirm_async
    confirm_async_bytes
  )
  DEFAULT_REQUIRED_STAGES = %w(encode_empty_publish_frames)

  def self.validate(doc : JSON::Any,
                    *,
                    min_metric_median : Float64 = 1.0,
                    min_stage_median : Float64 = 1.0,
                    required_metrics : Array(String) = DEFAULT_REQUIRED_METRICS,
                    required_stages : Array(String) = DEFAULT_REQUIRED_STAGES) : Array(String)
    errors = [] of String

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
        validate_numeric_lane(entry, errors, "metric #{name}", min_metric_median)
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
        validate_numeric_lane(entry, errors, "stage #{name}", min_stage_median)
      end
    end

    errors
  end

  private def self.object_field(root : JSON::Any, name : String, errors : Array(String)) : Hash(String, JSON::Any)?
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

  private def self.float_field(root : JSON::Any, name : String, errors : Array(String), context : String) : Float64?
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

  private def self.samples_field(root : JSON::Any, errors : Array(String), context : String) : Array(JSON::Any)?
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

  private def self.validate_numeric_lane(entry : JSON::Any,
                                         errors : Array(String),
                                         context : String,
                                         minimum : Float64) : Nil
    median = float_field(entry, "median", errors, context)
    if median && median < minimum
      errors << "#{context}: median #{median} below #{minimum}"
    end

    samples = samples_field(entry, errors, context)
    if samples
      errors << "#{context}: samples is empty" if samples.empty?
      samples.each_with_index do |sample, index|
        value = sample.as_f
        errors << "#{context}: sample #{index} is below #{minimum}" if value < minimum
      rescue
        errors << "#{context}: sample #{index} is not a number"
      end
    end
  end
end
