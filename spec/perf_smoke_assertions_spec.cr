require "./spec_helper"
require "../tools/perf_smoke_assertions"

private def smoke_benchmark_json : JSON::Any
  JSON.parse(%({
    "tool": "amqp-ng publish microbench",
    "metrics": {
      "publish_single": {"unit": "msg/s", "median": 120000.0, "samples": [110000.0, 120000.0]},
      "confirm_sync": {"unit": "msg/s", "median": 3200.0, "samples": [3100.0, 3200.0]},
      "confirm_async": {"unit": "msg/s", "median": 4100.0, "samples": [4000.0, 4100.0]},
      "confirm_async_bytes": {"unit": "msg/s", "median": 4300.0, "samples": [4200.0, 4300.0]}
    },
    "stages": {
      "encode_empty_publish_frames": {"unit": "ops/s", "median": 5000000.0, "samples": [4900000.0, 5000000.0]}
    }
  }))
end

describe AmqpPerfSmokeAssert do
  it "accepts the default required perf-smoke lanes" do
    AmqpPerfSmokeAssert.validate(smoke_benchmark_json).should be_empty
  end

  it "rejects artifacts missing the async bytes confirm lane by default" do
    doc = smoke_benchmark_json
    doc["metrics"].as_h.delete("confirm_async_bytes")

    AmqpPerfSmokeAssert.validate(doc).should contain("missing metric confirm_async_bytes")
  end

  it "allows callers to override the required metric list" do
    doc = smoke_benchmark_json
    doc["metrics"].as_h.delete("confirm_async_bytes")

    AmqpPerfSmokeAssert.validate(doc, required_metrics: %w(publish_single confirm_sync)).should be_empty
  end
end
