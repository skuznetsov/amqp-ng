require "./spec_helper"
require "digest/sha256"
require "set"

module DocsFalsifierLint
  extend self

  ROOT        = File.expand_path("..", __DIR__)
  DOCS_DIR    = File.join(ROOT, "docs")
  MATRIX_PATH = File.join(DOCS_DIR, "16-falsifier-matrix.md")

  # Existing doc debt. This pins the current set so new normative
  # sections cannot be added without a same-section falsifier reference.
  BASELINE_UNLINKED = Set{
    "5eba4f68b42ffd15", "c29ffe168e7303c3", "0bd3d9df193d91f4", "faea3533a1715c7c", "c6b3026a76a1d957", "8396ef0beb51711f",
    "45aec877c6345148", "63ad907106dc5e4d", "cefc43541382f237", "5043a37f3d56b643", "646a41acabfa6262", "28458b126126e70b",
    "5dd708846bf681dc", "896a90d238f7b418", "6956fa07d4220adc", "1230e3baa3305c09", "d5c0966c209b27fd", "067b55d14347807a",
    "5f4ec3f3e3006578", "42bb4a039288f0a9", "c6b725ca90283c2a", "f8ef58a5b30d7b1a", "fe11645e12698472", "71cea136a2d6b4cd",
    "be90d4d16eca8915", "27b5eb5db9689b84", "7506a1883c4b40dd", "2ce68e6357b52b53", "877413758e1f587b", "d9a16bca88eb445d",
    "1e6d29f2f156f2b7", "dfcfcff10251d5e8", "76b9024d5d4bba19", "7d009a2068c9c4c9", "737b428ee32cca81", "68bfe3b891053d63",
    "a7769e96b12f18d6", "0c59aa3ab291cdf8", "0443a1d2ed43089e", "59022d9804377fbb", "e22dfe594563fc51", "0c440fce0e3d1e05",
    "7c7fbb856c61c0bb", "3fc4d3adfb06334f", "8535458519e65679", "40960a38ca30c76e", "1dc52d1b34cbadc4", "2a56c05147befdc9",
    "74e735d951497876", "95e41639e18f2cda", "003480d1fd408907", "9cd7b165e333c8b5", "7d8e1b642237a707", "6c6d64842af782d9",
    "948f06eaec611334", "44f0eb1c9878c0cf", "c707da42465623a9", "62e8a1f3697108dc", "17ffca40b73b91da", "ece811f4337ed996",
    "8cae2f08e2beb280", "8c4d900227832711", "302c026408535bb4", "1325f2c12acf5f1e", "1b0becc81c0b29f1", "2f58e523f030dd2a",
    "cc68228762107f2c", "02dd22ed2038280b", "0839d5126ad759db", "f9a0ef48e999db1c", "c208e5cf06f41157", "16aceb420d2b3065",
    "d91c24bf704e0f68", "45495990b2365968", "3718bcda15a6040c", "0cb288a13ea58072", "e69b333fe75f3658", "a837900a34f27bc8",
    "ee44ca6dfe8601bc", "3f7354490a5aafa8", "a83407f02c6bd545", "272c4d574c131e37", "caa4af65bb121d87", "aba3e6ab5759635f",
    "e124fd989060c1ff", "13e010e2ffb71bbc", "b22d895595070045", "15e72b7d5597314a", "bbc136df135f19d2", "232a2df8895b7ad3",
    "c2ca444cf856a413", "7dadeaef0eeb7ef0", "a838cd040addab64", "8db091e84e4fc0ef", "07a66e1d35effff3", "24d60885a8172a39",
    "68591ec1f7fed3b4", "0e2b394414061bc2", "156149d3c35826b3", "3fa1fc77bd6a8dbb", "90cded6e4c93506a", "ddfa105fb03ca82a",
    "6832ca0284973b03", "71ad07c37e84ac04", "9c95c359a4028d88", "70990542534409f3", "456a54f72b07e898", "33269d733741ecb5",
    "04a6a538a3c8faf6", "23ef09cdc00427cb", "cadf97fdfd252289", "6bd8c1a1ca8909e3", "bd6e0970e8cce146", "4523b0c807c4299f",
    "2bfb23bccb861fc0", "9bd2ed0847a83b4c", "b85252328ea08e47", "728a5e5c7ca6ba4b", "9a8dee5ccc82a07d", "0673b49edd959910",
    "43a031c94ca778ee", "27f2e2931a39d14c", "fdc66d9f9f17d7e7", "b3e98c83a853d0d6", "65d9a5e9ba31f1d3", "65569a26fbbcc6d1",
    "7631d76bd10cef26", "9daaf0e12c865d47", "a8d164b63a714240", "b3e8bb061e339b79",
  }

  record Section, file : String, title : String, start_line : Int32, body : String do
    def key : String
      "#{file} :: #{title}"
    end

    def digest : String
      Digest::SHA256.hexdigest(key)[0, 16]
    end

    def label : String
      "#{file}:#{start_line} #{title} [#{digest}]"
    end
  end

  def docs : Array(String)
    Dir.glob(File.join(DOCS_DIR, "**/*.md")).sort.reject(MATRIX_PATH)
  end

  def sections(file : String) : Array(Section)
    relative = file.lchop("#{ROOT}/")
    result = [] of Section
    title = "(preamble)"
    start_line = 1
    body = [] of String
    in_fence = false

    flush = -> {
      result << Section.new(relative, title, start_line, body.join)
    }

    line_no = 0
    File.each_line(file) do |line|
      line_no += 1

      if line.starts_with?("```")
        in_fence = !in_fence
        body << line
        next
      end

      if !in_fence && line.starts_with?("#")
        flush.call
        title = line.strip
        start_line = line_no
        body = [line]
      elsif !in_fence
        body << line
      end
    end

    flush.call
    result
  end

  def normative?(section : Section) : Bool
    section.body.matches?(/\bMUST(?: NOT)?\b/)
  end

  def linked?(section : Section) : Bool
    section.body.matches?(/Falsifier(?:s)?:.*T-[A-Z0-9-]+/m)
  end

  def unlinked_normative_sections : Array(Section)
    docs.flat_map { |file| sections(file) }.select do |section|
      normative?(section) && !linked?(section)
    end
  end

  def token_pattern : Regex
    /(?:T-[A-Z0-9-]+-\d{3}(?:\.\.(?:\d{3}|N))?|T-[A-Z0-9-]+\*)/
  end

  def matrix_tokens : Set(String)
    File.read(MATRIX_PATH).scan(token_pattern).map(&.[0]).to_set
  end

  def wildcard_prefixes(tokens : Set(String)) : Set(String)
    tokens.select(&.ends_with?("*")).map do |token|
      prefix = token[0, token.size - 1]
      prefix = prefix[0, prefix.size - 1] if prefix.ends_with?("-")
      prefix
    end.to_set
  end

  def covered_by_range?(token : String, matrix : Set(String)) : Bool
    match = token.match(/^(.+)-(\d{3})(?:\.\.(\d{3}|N))?$/)
    return false unless match

    prefix = match[1]
    first = match[2].to_i
    last = match[3]?.try { |raw| raw == "N" ? Int32::MAX : raw.to_i } || first

    matrix.any? do |matrix_token|
      matrix_match = matrix_token.match(/^(.+)-(\d{3})\.\.(\d{3}|N)$/)
      next false unless matrix_match

      matrix_prefix = matrix_match[1]
      matrix_first = matrix_match[2].to_i
      matrix_last = matrix_match[3] == "N" ? Int32::MAX : matrix_match[3].to_i

      prefix == matrix_prefix && first >= matrix_first && last <= matrix_last
    end
  end

  def covered_by_wildcard?(token : String, prefixes : Set(String)) : Bool
    token = token.sub(/\.\..*$/, "")
    token = token[0, token.size - 1] if token.ends_with?("*")
    token = token[0, token.size - 1] if token.ends_with?("-")

    prefixes.any? { |prefix| token == prefix || token.starts_with?("#{prefix}-") }
  end

  def referenced_falsifiers : Array(Tuple(String, String))
    docs.flat_map do |file|
      sections(file).flat_map do |section|
        next [] of Tuple(String, String) unless section.body.includes?("Falsifier")

        section.body.scan(token_pattern).map do |match|
          {section.label, match[0]}
        end
      end
    end
  end

  def docs_uri_query_keys : Set(String)
    path = File.join(ROOT, "docs/04-uri-and-config.md")
    keys = Set(String).new
    in_table = false

    File.each_line(path) do |line|
      if line.starts_with?("| Key")
        in_table = true
        next
      end
      next unless in_table
      break if line.strip.empty?
      next if line.starts_with?("|---")

      if match = line.match(/^\| `([^`]+)`/)
        keys << match[1]
      end
    end

    keys
  end

  def readme_uri_query_keys : Set(String)
    path = File.join(ROOT, "README.md")
    keys = Set(String).new
    in_list = false

    File.each_line(path) do |line|
      if line.strip == "Supported URI query keys in v0:"
        in_list = true
        next
      end
      next unless in_list
      next if line.strip.empty? && keys.empty?
      break if line.strip.empty?

      if match = line.match(/^- `([^`]+)`/)
        keys << match[1]
      end
    end

    keys
  end
end

describe "docs falsifier links" do
  it "does not add new unlinked MUST/MUST NOT sections" do
    new_unlinked = DocsFalsifierLint.unlinked_normative_sections.reject do |section|
      DocsFalsifierLint::BASELINE_UNLINKED.includes?(section.digest)
    end

    if new_unlinked.any?
      fail "new normative sections need same-section Falsifier: T-* links:\n#{new_unlinked.map(&.label).join('\n')}"
    end
  end

  it "points explicit Falsifier references at the matrix" do
    matrix = DocsFalsifierLint.matrix_tokens
    wildcard_prefixes = DocsFalsifierLint.wildcard_prefixes(matrix)
    unknown = DocsFalsifierLint.referenced_falsifiers.reject do |label, token|
      matrix.includes?(token) ||
        DocsFalsifierLint.covered_by_range?(token, matrix) ||
        DocsFalsifierLint.covered_by_wildcard?(token, wildcard_prefixes)
    end

    if unknown.any?
      details = unknown.map { |label, token| "#{label}: #{token}" }.join('\n')
      fail "Falsifier references missing from docs/16-falsifier-matrix.md:\n#{details}"
    end
  end
end

describe "docs config surface" do
  it "keeps docs/04 URI query keys aligned with Config::RECOGNIZED_QUERY_KEYS" do
    DocsFalsifierLint.docs_uri_query_keys.should eq(Amqp::Config::RECOGNIZED_QUERY_KEYS.to_set)
  end

  it "keeps README URI query keys aligned with Config::RECOGNIZED_QUERY_KEYS" do
    DocsFalsifierLint.readme_uri_query_keys.should eq(Amqp::Config::RECOGNIZED_QUERY_KEYS.to_set)
  end
end
