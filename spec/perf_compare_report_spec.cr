require "./spec_helper"
require "../tools/perf_compare_report"

describe AmqpPerfCompareReport do
  it "classifies improvements, regressions, stable lanes, missing lanes, and new lanes" do
    baseline = JSON.parse(%({
      "metrics": {
        "publish_single": {"median": 100.0},
        "confirm_sync": {"median": 100.0},
        "stable": {"median": 100.0},
        "removed": {"median": 100.0}
      },
      "stages": {
        "encode": {"median": 200.0}
      }
    }))
    current = JSON.parse(%({
      "metrics": {
        "publish_single": {"median": 130.0},
        "confirm_sync": {"median": 70.0},
        "stable": {"median": 104.0},
        "added": {"median": 50.0}
      },
      "stages": {
        "encode": {"median": 150.0}
      }
    }))

    deltas = AmqpPerfCompareReport.compare(baseline, current, threshold_pct: 10.0)
    by_name = deltas.to_h { |delta| {delta.name, delta} }

    by_name["publish_single"].status.should eq(AmqpPerfCompareReport::Status::Improvement)
    by_name["confirm_sync"].status.should eq(AmqpPerfCompareReport::Status::Regression)
    by_name["stable"].status.should eq(AmqpPerfCompareReport::Status::Stable)
    by_name["removed"].status.should eq(AmqpPerfCompareReport::Status::MissingCurrent)
    by_name["added"].status.should eq(AmqpPerfCompareReport::Status::NewLane)
    by_name["encode"].status.should eq(AmqpPerfCompareReport::Status::Regression)
  end

  it "returns only threshold-crossing regression failures" do
    baseline = JSON.parse(%({
      "metrics": {
        "small": {"median": 100.0},
        "large": {"median": 100.0},
        "improved": {"median": 100.0}
      }
    }))
    current = JSON.parse(%({
      "metrics": {
        "small": {"median": 92.0},
        "large": {"median": 70.0},
        "improved": {"median": 130.0}
      }
    }))

    deltas = AmqpPerfCompareReport.compare(baseline, current, threshold_pct: 5.0)
    failures = AmqpPerfCompareReport.regression_failures(deltas, 15.0)

    failures.map(&.name).should eq(["large"])
  end

  it "renders compact human-readable lines" do
    baseline = JSON.parse(%({"metrics": {"lane": {"median": 100.0}}}))
    current = JSON.parse(%({"metrics": {"lane": {"median": 125.0}}}))

    AmqpPerfCompareReport.lines(AmqpPerfCompareReport.compare(baseline, current)).first
      .should eq("IMPROVEMENT metrics.lane: current 125.0 vs baseline 100.0 (+25.0%)")
  end
end
