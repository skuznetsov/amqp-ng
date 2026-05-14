require "./spec_helper"

# Iterates every fixture in spec/fixtures/frames/* and verifies that
# the bytes captured from a live RabbitMQ session decode cleanly with
# our Frame codec and re-encode to exactly the same bytes. This is the
# regression net for wire-format drift.
describe "frame corpus" do
  corpus_root = File.expand_path("fixtures/frames", __DIR__)

  Dir.children(corpus_root).sort!.each do |scenario|
    scenario_dir = File.join(corpus_root, scenario)
    next unless File.directory?(scenario_dir)

    describe scenario do
      it "client→server frames roundtrip" do
        path = File.join(scenario_dir, "c2s.bin")
        next unless File.exists?(path)
        bytes = File.read(path).to_slice

        offset = 0
        if bytes.size >= 8 && bytes[0, 8] == Amqp::Wire::PROTOCOL_HEADER
          offset = 8
        end

        verify_frames_roundtrip(bytes, offset, scenario, "c2s")
      end

      it "server→client frames roundtrip" do
        path = File.join(scenario_dir, "s2c.bin")
        next unless File.exists?(path)
        bytes = File.read(path).to_slice

        verify_frames_roundtrip(bytes, 0, scenario, "s2c")
      end
    end
  end
end

private def verify_frames_roundtrip(bytes : Bytes, offset : Int32,
                                    scenario : String, direction : String) : Nil
  count = 0
  while offset < bytes.size
    chunk_start = offset
    slice = bytes[offset, bytes.size - offset]
    io = IO::Memory.new(slice, false)

    frame = begin
      Amqp::Wire::Frame.read(io, 0_u32)
    rescue ex
      fail "#{scenario}/#{direction} frame ##{count + 1} at offset #{chunk_start} failed to decode: #{ex.class}: #{ex.message}"
    end

    consumed = io.pos.to_i32
    original = bytes[chunk_start, consumed]

    buf = IO::Memory.new
    frame.write(buf)
    encoded = buf.to_slice
    if encoded != original
      fail "#{scenario}/#{direction} frame ##{count + 1} at offset #{chunk_start} did not round-trip: " \
           "orig=#{original.hexstring}, re-encoded=#{encoded.hexstring}"
    end

    offset += consumed
    count += 1
  end
  count.should be > 0, "#{scenario}/#{direction} decoded zero frames"
end
