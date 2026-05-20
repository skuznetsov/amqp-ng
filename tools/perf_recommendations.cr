require "json"

module AmqpPerfRecommendations
  extend self

  record Recommendation, title : String, body : String

  def analyze(doc : JSON::Any) : Array(Recommendation)
    metrics = doc["metrics"]?.try(&.as_h?)
    return [Recommendation.new("No metrics", "benchmark JSON has no metrics object")] unless metrics

    recommendations = [] of Recommendation
    add_batch_recommendation(metrics, recommendations)
    add_fixed_route_recommendation(metrics, recommendations)
    add_body_size_recommendation(metrics, recommendations)
    recommendations
  end

  private def add_batch_recommendation(metrics : Hash(String, JSON::Any),
                                       recommendations : Array(Recommendation)) : Nil
    single = median(metrics, "publish_single")
    batch = median(metrics, "publish_batch_bytes") || median(metrics, "publish_batch")
    return unless single && batch

    if batch >= single * 1.20
      recommendations << Recommendation.new(
        "Use batch publish for throughput",
        "batch publish is #{ratio(batch, single)}x the single-publish lane in this run; prefer publish_batch(Array(Bytes)) for repeated same-route fire-and-forget publishing.",
      )
    end
  end

  private def add_fixed_route_recommendation(metrics : Hash(String, JSON::Any),
                                             recommendations : Array(Recommendation)) : Nil
    repeat = median(metrics, "publish_single_repeat") || median(metrics, "publish_single")
    prepared_empty = median(metrics, "publish_prepared_empty_body")
    rr2 = median(metrics, "publish_single_round_robin_routes_2") || median(metrics, "publish_single_alternating_routes")
    rr8 = median(metrics, "publish_single_round_robin_routes_8")

    if repeat && prepared_empty && prepared_empty >= repeat * 1.20
      recommendations << Recommendation.new(
        "Use prepared publishers for fixed routes",
        "the prepared empty-body lane is #{ratio(prepared_empty, repeat)}x the repeated single-publish lane in this run; prefer prepared_publisher for stable fire-and-forget routes.",
      )
    end

    return unless rr2 && rr8 && rr8 < rr2 * 0.80

    recommendations << Recommendation.new(
      "Avoid high route fanout on one hot publisher",
      "8-route round-robin throughput is #{ratio(rr8, rr2)}x the 2-route lane in this run; shard by route, batch per route, or keep one prepared publisher per hot route.",
    )
  end

  private def add_body_size_recommendation(metrics : Hash(String, JSON::Any),
                                           recommendations : Array(Recommendation)) : Nil
    body0 = median(metrics, "publish_single_body_0") || median(metrics, "publish_single_empty_body")
    body1024 = median(metrics, "publish_single_body_1024")
    return unless body0 && body1024 && body1024 < body0 * 0.50

    recommendations << Recommendation.new(
      "Body bytes dominate this publish lane",
      "the 1024-byte lane is #{ratio(body1024, body0)}x the empty-body lane in this run; reduce body size, batch, or use multiple connections before chasing smaller codec micro-optimizations.",
    )
  end

  private def median(metrics : Hash(String, JSON::Any), name : String) : Float64?
    metrics[name]?.try &.["median"].as_f?
  rescue
    nil
  end

  private def ratio(numerator : Float64, denominator : Float64) : String
    return "inf" if denominator == 0
    (numerator / denominator).round(2).to_s
  end
end
