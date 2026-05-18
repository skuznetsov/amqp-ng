require "./spec_helper"
require "../src/amqp/arguments"

describe Amqp::Arguments do
  it "coerces NamedTuple arguments with string keys and exact supported scalar widths" do
    args = Amqp.coerce_arguments({
      count:    1_i16,
      priority: 2_u8,
      enabled:  true,
      label:    "jobs",
      nested:   {limit: 10_i32},
      list:     [1_i32, "two"],
    })

    args["count"].should eq(1_i16)
    args["priority"].should eq(2_u8)
    args["enabled"].should eq(true)
    args["label"].should eq("jobs")

    nested = args["nested"].as(Hash(String, Amqp::FieldValue))
    nested["limit"].should eq(10_i32)

    list = args["list"].as(Array(Amqp::FieldValue))
    list.should eq([1_i32, "two"] of Amqp::FieldValue)
  end

  it "returns existing Amqp::Arguments without copying" do
    args = Amqp::Arguments{"x" => 1_i32}

    Amqp.coerce_arguments(args).object_id.should eq(args.object_id)
  end
end
