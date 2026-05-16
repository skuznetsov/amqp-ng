require "../spec_helper"

alias TX = Amqp::Wire::AmqpZeroNineOne::TxMethods

describe Amqp::Wire::AmqpZeroNineOne::TxMethods do
  it "encodes tx.select, tx.commit, and tx.rollback" do
    TX::Select.new.to_payload.should eq(Bytes[0x00, 0x5a, 0x00, 0x0a])
    TX::Commit.new.to_payload.should eq(Bytes[0x00, 0x5a, 0x00, 0x14])
    TX::Rollback.new.to_payload.should eq(Bytes[0x00, 0x5a, 0x00, 0x1e])
    TX::SelectOk.read(IO::Memory.new).should be_a(TX::SelectOk)
    TX::CommitOk.read(IO::Memory.new).should be_a(TX::CommitOk)
    TX::RollbackOk.read(IO::Memory.new).should be_a(TX::RollbackOk)
  end
end
