require "./spec_helper"
require "../tools/perf_thresholds"

private def benchmark_json : JSON::Any
  JSON.parse(%({
    "tool": "amqp-ng publish microbench",
    "metrics": {
      "publish_single": {"unit": "msg/s", "median": 120000.0, "samples": [110000.0, 120000.0, 130000.0]},
      "confirm_sync": {"unit": "msg/s", "median": 3200.0, "samples": [3100.0, 3200.0, 3300.0]}
    },
    "stages": {
      "encode_empty_publish_frames": {"unit": "ops/s", "median": 5000000.0, "samples": [4900000.0, 5000000.0, 5100000.0]}
    }
  }))
end

describe AmqpPerfThresholds do
  it "accepts a benchmark that satisfies lane thresholds" do
    thresholds = JSON.parse(%({
      "profile": "spec",
      "metrics": {
        "publish_single": {"median_min": 100000.0, "sample_min": 100000.0},
        "confirm_sync": {"median_min": 3000.0}
      },
      "stages": {
        "encode_empty_publish_frames": {"median_min": 4000000.0}
      }
    }))

    AmqpPerfThresholds.validate(benchmark_json, thresholds).should be_empty
  end

  it "reports missing lanes and below-threshold medians" do
    thresholds = JSON.parse(%({
      "profile": "spec",
      "metrics": {
        "publish_single": {"median_min": 150000.0},
        "missing_lane": {"median_min": 1.0}
      }
    }))

    errors = AmqpPerfThresholds.validate(benchmark_json, thresholds)
    errors.should contain("metrics.publish_single: median 120000.0 below median_min 150000.0")
    errors.should contain("metrics.missing_lane: missing benchmark lane")
  end

  it "requires at least one explicit threshold" do
    thresholds = JSON.parse(%({"profile": "empty"}))

    AmqpPerfThresholds.validate(benchmark_json, thresholds).should contain("threshold profile does not define any metric or stage thresholds")
  end

  it "reports malformed threshold sections" do
    thresholds = JSON.parse(%({"metrics": ["publish_single"]}))

    AmqpPerfThresholds.validate(benchmark_json, thresholds).should contain("threshold profile: metrics must be an object")
  end
end
