require "./spec_helper"
require "yaml"

module DependencyHygieneSpec
  extend self

  ROOT = File.expand_path("..", __DIR__)

  def yaml(path : String) : YAML::Any
    YAML.parse(File.read(File.join(ROOT, path)))
  end

  def mapping_keys(node : YAML::Any?) : Array(String)
    return [] of String unless node

    node.as_h.keys.map(&.as_s).sort
  end
end

describe "dependency hygiene (T-API-DEPS-001)" do
  it "declares no runtime dependencies in shard.yml" do
    shard = DependencyHygieneSpec.yaml("shard.yml")
    deps = DependencyHygieneSpec.mapping_keys(shard["dependencies"]?)

    deps.should be_empty, "runtime dependencies in shard.yml: #{deps.join(", ")}"
  end

  it "has no locked shards when no development dependencies exist" do
    shard = DependencyHygieneSpec.yaml("shard.yml")
    dev_deps = DependencyHygieneSpec.mapping_keys(shard["development_dependencies"]?)
    lock_path = File.join(DependencyHygieneSpec::ROOT, "shard.lock")

    next unless File.exists?(lock_path) && dev_deps.empty?

    lock = DependencyHygieneSpec.yaml("shard.lock")
    locked = DependencyHygieneSpec.mapping_keys(lock["shards"]?)

    locked.should be_empty, "unexpected locked shards without runtime/dev dependencies: #{locked.join(", ")}"
  end
end
