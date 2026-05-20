require "json"

module AmqpPerfThresholds
  extend self

  alias Errors = Array(String)

  def validate(benchmark : JSON::Any, thresholds : JSON::Any) : Errors
    errors = [] of String

    tool = benchmark["tool"]?.try(&.as_s?)
    errors << "benchmark: unexpected tool #{tool.inspect}" unless tool == "amqp-ng publish microbench"

    threshold_count = 0
    threshold_count += validate_section(benchmark, thresholds, "metrics", errors)
    threshold_count += validate_section(benchmark, thresholds, "stages", errors)

    errors << "threshold profile does not define any metric or stage thresholds" if threshold_count == 0
    errors
  end

  private def validate_section(benchmark : JSON::Any,
                               thresholds : JSON::Any,
                               section : String,
                               errors : Errors) : Int32
    section_value = thresholds[section]?
    return 0 unless section_value

    section_thresholds = section_value.as_h?
    unless section_thresholds
      errors << "threshold profile: #{section} must be an object"
      return 0
    end

    entries = benchmark[section]?.try(&.as_h?)
    unless entries
      errors << "benchmark: missing #{section}"
      return section_thresholds.size
    end

    section_thresholds.each do |name, rule|
      entry = entries[name]?
      unless entry
        errors << "#{section}.#{name}: missing benchmark lane"
        next
      end

      validate_rule(section, name, entry, rule, errors)
    end

    section_thresholds.size
  end

  private def validate_rule(section : String,
                            name : String,
                            entry : JSON::Any,
                            rule : JSON::Any,
                            errors : Errors) : Nil
    rule_object = rule.as_h?
    unless rule_object
      errors << "#{section}.#{name}: threshold rule must be an object"
      return
    end

    median_min = optional_float(rule_object, "median_min", "#{section}.#{name}", errors)
    sample_min = optional_float(rule_object, "sample_min", "#{section}.#{name}", errors)
    if median_min.nil? && sample_min.nil?
      errors << "#{section}.#{name}: threshold rule must define median_min or sample_min"
      return
    end

    median = required_float(entry, "median", "#{section}.#{name}", errors)
    if median && median_min && median < median_min
      errors << "#{section}.#{name}: median #{median} below median_min #{median_min}"
    end

    samples = entry["samples"]?.try(&.as_a?)
    unless samples
      errors << "#{section}.#{name}: missing samples"
      return
    end
    errors << "#{section}.#{name}: samples is empty" if samples.empty?

    if sample_min
      samples.each_with_index do |sample, index|
        value = sample.as_f
        errors << "#{section}.#{name}: sample #{index} #{value} below sample_min #{sample_min}" if value < sample_min
      rescue
        errors << "#{section}.#{name}: sample #{index} is not a number"
      end
    end
  end

  private def optional_float(object : Hash(String, JSON::Any),
                             key : String,
                             context : String,
                             errors : Errors) : Float64?
    value = object[key]?
    return nil unless value
    value.as_f
  rescue
    errors << "#{context}: #{key} must be a number"
    nil
  end

  private def required_float(object : JSON::Any,
                             key : String,
                             context : String,
                             errors : Errors) : Float64?
    value = object[key]?
    unless value
      errors << "#{context}: missing #{key}"
      return nil
    end
    value.as_f
  rescue
    errors << "#{context}: #{key} must be a number"
    nil
  end
end
