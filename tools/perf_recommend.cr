require "json"
require "./perf_recommendations"

path = ARGV[0]? || "-"
raw = path == "-" ? STDIN.gets_to_end : File.read(path)
doc = JSON.parse(raw)

recommendations = AmqpPerfRecommendations.analyze(doc)
if recommendations.empty?
  puts "No performance recommendation crossed the configured heuristics."
else
  recommendations.each do |recommendation|
    puts "- #{recommendation.title}: #{recommendation.body}"
  end
end
