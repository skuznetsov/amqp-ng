require "./arguments"

module Amqp
  # Content properties carried by basic.publish/deliver/return/get-ok.
  # Layout per docs/05-wire-0-9-1/03-content-properties.md.
  struct Properties
    enum Persistence : UInt8
      Transient  = 1
      Persistent = 2
    end

    property content_type : String?
    property content_encoding : String?
    property headers : Amqp::Arguments?
    property delivery_mode : Persistence?
    property priority : UInt8?
    property correlation_id : String?
    property reply_to : String?
    property expiration : String?
    property message_id : String?
    property timestamp : Time?
    property type : String?
    property user_id : String?
    property app_id : String?
    property cluster_id : String?

    def initialize(
      @content_type = nil,
      @content_encoding = nil,
      @headers = nil,
      @delivery_mode = nil,
      persistence : Persistence? = nil,
      @priority = nil,
      @correlation_id = nil,
      @reply_to = nil,
      @expiration = nil,
      @message_id = nil,
      @timestamp = nil,
      @type = nil,
      @user_id = nil,
      @app_id = nil,
      @cluster_id = nil,
    )
      @delivery_mode = persistence || @delivery_mode
    end

    def persistence : Persistence?
      @delivery_mode
    end

    def empty? : Bool
      @content_type.nil? && @content_encoding.nil? && @headers.nil? &&
        @delivery_mode.nil? && @priority.nil? && @correlation_id.nil? &&
        @reply_to.nil? && @expiration.nil? && @message_id.nil? &&
        @timestamp.nil? && @type.nil? && @user_id.nil? &&
        @app_id.nil? && @cluster_id.nil?
    end
  end
end
