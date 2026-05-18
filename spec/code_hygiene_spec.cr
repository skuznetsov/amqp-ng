require "./spec_helper"

module CodeHygieneSpec
  extend self

  ROOT = File.expand_path("..", __DIR__)

  def crystal_files_under(*dirs : String) : Array(String)
    dirs.flat_map do |dir|
      Dir.glob(File.join(ROOT, dir, "**/*.cr"))
    end.sort
  end

  def matching_lines(files : Enumerable(String), needle : String) : Array(String)
    matches = [] of String
    files.each do |file|
      line_no = 0
      File.each_line(file) do |line|
        line_no += 1
        matches << "#{file.lchop("#{ROOT}/")}:#{line_no}: #{line.strip}" if line.includes?(needle)
      end
    end
    matches
  end

  def matching_lines(files : Enumerable(String), pattern : Regex) : Array(String)
    matches = [] of String
    files.each do |file|
      line_no = 0
      File.each_line(file) do |line|
        line_no += 1
        matches << "#{file.lchop("#{ROOT}/")}:#{line_no}: #{line.strip}" if line.matches?(pattern)
      end
    end
    matches
  end
end

describe "source hygiene" do
  it "does not reach into private ivars from shard or spec code" do
    offenders = CodeHygieneSpec.matching_lines(
      CodeHygieneSpec.crystal_files_under("src", "spec"),
      "." + "@"
    )

    offenders.should be_empty, offenders.join('\n')
  end

  it "keeps the public implementation free of module class-variable state (T-API-NOGLOBAL-001)" do
    offenders = CodeHygieneSpec.matching_lines(
      CodeHygieneSpec.crystal_files_under("src"),
      /^\s*@@/
    )

    offenders.should be_empty, offenders.join('\n')
  end

  it "does not use stdlib APIs with known active deprecation warnings" do
    offenders = CodeHygieneSpec.matching_lines(
      CodeHygieneSpec.crystal_files_under("src", "spec", "tools"),
      "Time" + ".monotonic"
    )

    offenders.should be_empty, offenders.join('\n')
  end

  it "keeps the wire codec free of fibers, sockets, TLS, and module state" do
    wire_files = CodeHygieneSpec.crystal_files_under("src/amqp/wire")
    forbidden = {
      /\bspawn\b/     => "fiber spawn",
      /\bFiber\b/     => "fiber API",
      /\bTCPSocket\b/ => "TCP socket",
      /\bSocket\b/    => "socket API",
      /\bOpenSSL\b/   => "TLS/OpenSSL API",
      /@@/            => "class/module variable state",
    }

    offenders = forbidden.flat_map do |pattern, label|
      CodeHygieneSpec.matching_lines(wire_files, pattern).map do |line|
        "#{label}: #{line}"
      end
    end

    offenders.should be_empty, offenders.join('\n')
  end
end
