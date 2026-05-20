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

  it "warns when stable benchmark metadata differs" do
    baseline = JSON.parse(%({
      "benchmark_schema_version": 2,
      "environment": {
        "crystal_version": "1.20.1",
        "crystal_description": "Crystal 1.20.1",
        "compile_flags": {"release": true, "preview_mt": false, "execution_context": false}
      }
    }))
    current = JSON.parse(%({
      "benchmark_schema_version": 2,
      "environment": {
        "crystal_version": "1.20.2",
        "crystal_description": "Crystal 1.20.2",
        "compile_flags": {"release": true, "preview_mt": true, "execution_context": false}
      }
    }))

    warnings = AmqpPerfCompareReport.metadata_warnings(baseline, current)
    warnings.should contain(%(metadata environment.crystal_version differs: baseline "1.20.1", current "1.20.2"))
    warnings.should contain(%(metadata environment.compile_flags.preview_mt differs: baseline false, current true))
  end

  it "warns when metadata exists only on one side" do
    baseline = JSON.parse(%({"benchmark_schema_version": 2}))
    current = JSON.parse(%({"metrics": {"lane": {"median": 1.0}}}))

    AmqpPerfCompareReport.metadata_warnings(baseline, current)
      .should contain("metadata benchmark_schema_version exists only in baseline")
  end

  it "warns when benchmark workload metadata differs" do
    baseline = JSON.parse(%({
      "url": "amqp://127.0.0.1:5672/",
      "publish_n": 1000,
      "body_bytes": 256,
      "confirm_windows": [1, 10, 100],
      "metrics": {"lane": {"median": 1.0}}
    }))
    current = JSON.parse(%({
      "url": "amqp://127.0.0.1:5673/",
      "publish_n": 2000,
      "body_bytes": 1024,
      "confirm_windows": [1, 50, 500],
      "metrics": {"lane": {"median": 1.0}}
    }))

    warnings = AmqpPerfCompareReport.metadata_warnings(baseline, current)
    warnings.should contain(%(metadata url differs: baseline "amqp://127.0.0.1:5672/", current "amqp://127.0.0.1:5673/"))
    warnings.should contain("metadata publish_n differs: baseline 1000, current 2000")
    warnings.should contain("metadata body_bytes differs: baseline 256, current 1024")
    warnings.should contain("metadata confirm_windows differs: baseline [1,10,100], current [1,50,500]")
  end

  it "promotes metadata warnings to failures only when requested" do
    warnings = ["metadata body_bytes differs: baseline 256, current 1024"]

    AmqpPerfCompareReport.metadata_failures(warnings, false).should be_empty
    AmqpPerfCompareReport.metadata_failures(warnings, true).should eq(warnings)
  end
end
