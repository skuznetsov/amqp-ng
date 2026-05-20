require "./spec_helper"
require "../tools/perf_recommendations"

describe AmqpPerfRecommendations do
  it "recommends batch, prepared fixed-route, and body-size followups from benchmark JSON" do
    doc = JSON.parse(%({
      "tool": "amqp-ng publish microbench",
      "metrics": {
        "publish_single": {"median": 300000.0, "samples": [300000.0]},
        "publish_single_repeat": {"median": 320000.0, "samples": [320000.0]},
        "publish_batch_bytes": {"median": 600000.0, "samples": [600000.0]},
        "publish_prepared_empty_body": {"median": 500000.0, "samples": [500000.0]},
        "publish_single_round_robin_routes_2": {"median": 500000.0, "samples": [500000.0]},
        "publish_single_round_robin_routes_8": {"median": 300000.0, "samples": [300000.0]},
        "publish_single_body_0": {"median": 800000.0, "samples": [800000.0]},
        "publish_single_body_1024": {"median": 250000.0, "samples": [250000.0]}
      }
    }))

    titles = AmqpPerfRecommendations.analyze(doc).map(&.title)
    titles.should contain("Use batch publish for throughput")
    titles.should contain("Use prepared publishers for fixed routes")
    titles.should contain("Avoid high route fanout on one hot publisher")
    titles.should contain("Body bytes dominate this publish lane")
  end

  it "stays quiet when no heuristic crosses its threshold" do
    doc = JSON.parse(%({
      "tool": "amqp-ng publish microbench",
      "metrics": {
        "publish_single": {"median": 100000.0, "samples": [100000.0]},
        "publish_batch_bytes": {"median": 105000.0, "samples": [105000.0]},
        "publish_single_body_0": {"median": 100000.0, "samples": [100000.0]},
        "publish_single_body_1024": {"median": 90000.0, "samples": [90000.0]}
      }
    }))

    AmqpPerfRecommendations.analyze(doc).should be_empty
  end
end
