module Amqp
  alias FieldValue = Nil |
                     Bool |
                     Int8 | UInt8 | Int16 | UInt16 | Int32 | UInt32 | Int64 |
                     Float32 | Float64 |
                     Bytes | String |
                     Time |
                     Array(FieldValue) |
                     Hash(String, FieldValue)

  alias Arguments = Hash(String, FieldValue)

  def self.coerce_arguments(arguments : Arguments) : Arguments
    arguments
  end

  def self.coerce_arguments(arguments : NamedTuple) : Arguments
    converted = Arguments.new
    arguments.each do |key, value|
      converted[key.to_s] = coerce_field(value)
    end
    converted
  end

  def self.coerce_field(value : Nil) : FieldValue
    value
  end

  def self.coerce_field(value : Bool) : FieldValue
    value
  end

  def self.coerce_field(value : Int8 | UInt8 | Int16 | UInt16 | Int32 | UInt32 | Int64) : FieldValue
    value
  end

  def self.coerce_field(value : Float32 | Float64) : FieldValue
    value
  end

  def self.coerce_field(value : Bytes | String | Time) : FieldValue
    value
  end

  def self.coerce_field(value : NamedTuple) : FieldValue
    coerce_arguments(value)
  end

  def self.coerce_field(value : Array) : FieldValue
    converted = [] of FieldValue
    value.each { |item| converted << coerce_field(item) }
    converted
  end

  def self.coerce_field(value : Hash) : FieldValue
    converted = Arguments.new
    value.each do |key, item|
      converted[key.to_s] = coerce_field(item)
    end
    converted
  end

  def self.coerce_field(value : Int) : FieldValue
    value.to_i64
  end

  def self.coerce_field(value : Float) : FieldValue
    value.to_f64
  end

  def self.coerce_field(value) : FieldValue
    value.to_s
  end
end
