require "json"

module AmqpPerfCompareReport
  extend self

  enum Status
    Regression
    Improvement
    Stable
    MissingCurrent
    NewLane
  end

  record Delta,
    section : String,
    name : String,
    baseline : Float64?,
    current : Float64?,
    ratio : Float64?,
    pct : Float64?,
    status : Status

  def compare(baseline : JSON::Any, current : JSON::Any, threshold_pct : Float64 = 10.0) : Array(Delta)
    deltas = [] of Delta
    compare_section("metrics", baseline, current, threshold_pct, deltas)
    compare_section("stages", baseline, current, threshold_pct, deltas)
    deltas.sort! { |left, right| magnitude(right) <=> magnitude(left) }
    deltas
  end

  def regression_failures(deltas : Array(Delta), fail_regression_pct : Float64?) : Array(Delta)
    return [] of Delta unless fail_regression_pct
    deltas.select do |delta|
      delta.status.regression? && delta.pct.try { |pct| pct <= -fail_regression_pct }
    end
  end

  def missing_current_failures(deltas : Array(Delta), fail_missing_current : Bool) : Array(Delta)
    return [] of Delta unless fail_missing_current
    deltas.select(&.status.missing_current?)
  end

  def new_lane_failures(deltas : Array(Delta), fail_new_lane : Bool) : Array(Delta)
    return [] of Delta unless fail_new_lane
    deltas.select(&.status.new_lane?)
  end

  def lines(deltas : Array(Delta)) : Array(String)
    return ["No comparable benchmark metrics or stages found."] if deltas.empty?

    deltas.map do |delta|
      case delta.status
      in .regression?, .improvement?, .stable?
        "#{status_label(delta.status)} #{delta.section}.#{delta.name}: current #{format(delta.current)} vs baseline #{format(delta.baseline)} (#{format_pct(delta.pct)})"
      in .missing_current?
        "MISSING_CURRENT #{delta.section}.#{delta.name}: baseline #{format(delta.baseline)}"
      in .new_lane?
        "NEW_LANE #{delta.section}.#{delta.name}: current #{format(delta.current)}"
      end
    end
  end

  def metadata_warnings(baseline : JSON::Any, current : JSON::Any) : Array(String)
    warnings = [] of String
    compare_metadata_value(warnings, "benchmark_schema_version", baseline["benchmark_schema_version"]?, current["benchmark_schema_version"]?)
    compare_metadata_value(warnings, "environment.crystal_version", environment_value(baseline, "crystal_version"), environment_value(current, "crystal_version"))
    compare_metadata_value(warnings, "environment.crystal_description", environment_value(baseline, "crystal_description"), environment_value(current, "crystal_description"))
    compare_metadata_value(warnings, "environment.compile_flags.release", compile_flag(baseline, "release"), compile_flag(current, "release"))
    compare_metadata_value(warnings, "environment.compile_flags.preview_mt", compile_flag(baseline, "preview_mt"), compile_flag(current, "preview_mt"))
    compare_metadata_value(warnings, "environment.compile_flags.execution_context", compile_flag(baseline, "execution_context"), compile_flag(current, "execution_context"))
    compare_workload_metadata(warnings, baseline, current)
    compare_lane_units(warnings, "metrics", baseline, current)
    compare_lane_units(warnings, "stages", baseline, current)
    warnings
  end

  def metadata_failures(warnings : Array(String), fail_metadata : Bool) : Array(String)
    fail_metadata ? warnings : [] of String
  end

  private def compare_section(section : String,
                              baseline : JSON::Any,
                              current : JSON::Any,
                              threshold_pct : Float64,
                              deltas : Array(Delta)) : Nil
    baseline_entries = section_entries(baseline, section)
    current_entries = section_entries(current, section)
    names = (baseline_entries.keys + current_entries.keys).uniq!.sort!

    names.each do |name|
      baseline_value = median(baseline_entries[name]?)
      current_value = median(current_entries[name]?)

      if baseline_value && current_value
        ratio = baseline_value == 0.0 ? nil : current_value / baseline_value
        pct = ratio.try { |value| (value - 1.0) * 100.0 }
        status =
          if pct && pct <= -threshold_pct
            Status::Regression
          elsif pct && pct >= threshold_pct
            Status::Improvement
          else
            Status::Stable
          end
        deltas << Delta.new(section, name, baseline_value, current_value, ratio, pct, status)
      elsif baseline_value
        deltas << Delta.new(section, name, baseline_value, nil, nil, nil, Status::MissingCurrent)
      elsif current_value
        deltas << Delta.new(section, name, nil, current_value, nil, nil, Status::NewLane)
      end
    end
  end

  private def section_entries(doc : JSON::Any, section : String) : Hash(String, JSON::Any)
    doc[section]?.try(&.as_h?) || {} of String => JSON::Any
  end

  private def median(entry : JSON::Any?) : Float64?
    entry.try(&.["median"].as_f?)
  rescue
    nil
  end

  private def environment_value(doc : JSON::Any, key : String) : JSON::Any?
    doc["environment"]?.try(&.[key]?)
  rescue
    nil
  end

  private def compile_flag(doc : JSON::Any, key : String) : JSON::Any?
    doc["environment"]?.try(&.["compile_flags"]?).try(&.[key]?)
  rescue
    nil
  end

  private def compare_metadata_value(warnings : Array(String),
                                     path : String,
                                     baseline : JSON::Any?,
                                     current : JSON::Any?) : Nil
    return unless baseline || current

    unless baseline && current
      warnings << "metadata #{path} exists only in #{baseline ? "baseline" : "current"}"
      return
    end

    baseline_value = baseline.to_json
    current_value = current.to_json
    return if baseline_value == current_value

    warnings << "metadata #{path} differs: baseline #{baseline_value}, current #{current_value}"
  end

  private def compare_workload_metadata(warnings : Array(String),
                                        baseline : JSON::Any,
                                        current : JSON::Any) : Nil
    [
      "url",
      "publish_n",
      "confirm_n",
      "batch_size",
      "samples",
      "body_bytes",
      "stage_n",
      "channel_counts",
      "connection_counts",
      "confirm_windows",
      "route_counts",
      "body_sweep_sizes",
      "consume_buffer",
    ].each do |key|
      compare_metadata_value(warnings, key, baseline[key]?, current[key]?)
    end
  end

  private def compare_lane_units(warnings : Array(String),
                                 section : String,
                                 baseline : JSON::Any,
                                 current : JSON::Any) : Nil
    baseline_entries = section_entries(baseline, section)
    current_entries = section_entries(current, section)
    common_names = (baseline_entries.keys & current_entries.keys).sort!

    common_names.each do |name|
      compare_metadata_value(
        warnings,
        "#{section}.#{name}.unit",
        baseline_entries[name]["unit"]?,
        current_entries[name]["unit"]?,
      )
    rescue
      next
    end
  end

  private def magnitude(delta : Delta) : Float64
    delta.pct.try(&.abs) || Float64::INFINITY
  end

  private def status_label(status : Status) : String
    case status
    in .regression?
      "REGRESSION"
    in .improvement?
      "IMPROVEMENT"
    in .stable?
      "STABLE"
    in .missing_current?
      "MISSING_CURRENT"
    in .new_lane?
      "NEW_LANE"
    end
  end

  private def format(value : Float64?) : String
    return "n/a" unless value
    value.round(2).to_s
  end

  private def format_pct(value : Float64?) : String
    return "n/a" unless value
    rounded = value.round(1)
    sign = rounded > 0 ? "+" : ""
    "#{sign}#{rounded}%"
  end
end
