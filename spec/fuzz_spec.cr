require "./spec_helper"

# Light fuzz harness: feed random bytes to the wire decoders and assert
# the codec either succeeds cleanly or raises a typed AMQP/IO error.
# A panic, infinite loop, or generic Exception escaping the codec is a
# bug — we trust internal contracts to bound badness.
#
# Iteration counts are deliberately modest so the suite stays fast in
# CI. Bump AMQP_FUZZ_ITERS=N locally for a deeper sweep.
private ITERS = (ENV["AMQP_FUZZ_ITERS"]? || "2000").to_i

# Typed errors the codecs are allowed to raise on invalid input.
private alias FuzzSafe = Amqp::Error | IO::Error | OverflowError | ArgumentError

describe "wire fuzz" do
  it "Frame.read tolerates random bytes" do
    rng = Random.new(0x1234_5678_u64)
    survived = 0
    ITERS.times do |i|
      size = rng.rand(0..512)
      bytes = Bytes.new(size)
      rng.random_bytes(bytes)
      io = IO::Memory.new(bytes, false)
      begin
        Amqp::Wire::Frame.read(io, 0_u32)
        survived += 1
      rescue FuzzSafe
        # Expected typed failure for malformed bytes.
      rescue ex
        fail "Frame.read raised non-typed #{ex.class} on iter #{i}: #{ex.message}"
      end
    end
    # Expect at least *some* iterations to fail decode; if every random
    # byte sequence parsed as a valid frame, the codec is suspiciously
    # lenient.
    survived.should be < ITERS
  end

  it "ContentHeader.decode tolerates random bytes" do
    rng = Random.new(0xDEAD_BEEF_u64)
    ITERS.times do |i|
      size = rng.rand(0..256)
      bytes = Bytes.new(size)
      rng.random_bytes(bytes)
      begin
        Amqp::Wire::AmqpZeroNineOne::ContentHeader.decode(bytes)
      rescue FuzzSafe
        # ok
      rescue ex
        fail "ContentHeader.decode raised non-typed #{ex.class} on iter #{i}: #{ex.message}"
      end
    end
  end

  it "Types.read_field_table tolerates random bytes" do
    rng = Random.new(0xFEED_FACE_u64)
    ITERS.times do |i|
      # Bound the embedded length so single iterations don't allocate
      # gigabytes: prepend a small explicit length prefix.
      payload_size = rng.rand(0..256)
      payload = Bytes.new(payload_size)
      rng.random_bytes(payload)

      buf = IO::Memory.new
      buf.write_bytes(payload_size.to_u32, IO::ByteFormat::NetworkEndian)
      buf.write(payload)
      buf.rewind

      begin
        Amqp::Wire::AmqpZeroNineOne::Types.read_field_table(buf)
      rescue FuzzSafe
        # ok
      rescue ex
        fail "Types.read_field_table raised non-typed #{ex.class} on iter #{i}: #{ex.message}"
      end
    end
  end
end
