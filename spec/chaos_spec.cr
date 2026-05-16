require "./spec_helper"

module ChaosHelper
  extend self

  def container : String?
    ENV["AMQP_CHAOS_DOCKER_CONTAINER"]?
  end

  def docker_available? : Bool
    Process.run("docker", ["--version"], output: Process::Redirect::Close,
      error: Process::Redirect::Close).success?
  rescue
    false
  end

  def docker(*args : String) : Nil
    status = Process.run("docker", args.to_a)
    raise "docker #{args.join(' ')} failed" unless status.success?
  end
end

describe "broker chaos fixtures" do
  it "detects heartbeat death when the broker process is paused" do
    container = ChaosHelper.container
    pending! "set AMQP_CHAOS_DOCKER_CONTAINER to run broker-pause chaos" unless container
    pending! "docker unavailable" unless ChaosHelper.docker_available?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    conn = Amqp.connect(SpecHelper.amqp_url, heartbeat: 1.second)
    begin
      ChaosHelper.docker("pause", container)
      deadline = Time.instant + 5.seconds
      until conn.closed?
        fail "connection did not close after broker pause" if Time.instant > deadline
        sleep 50.milliseconds
      end
      conn.close_reason.should be_a(Amqp::HeartbeatTimeoutError)
    ensure
      ChaosHelper.docker("unpause", container) rescue nil
      conn.close rescue nil
    end
  end

  it "recovers across a real broker container restart" do
    container = ChaosHelper.container
    pending! "set AMQP_CHAOS_DOCKER_CONTAINER to run broker-restart chaos" unless container
    pending! "docker unavailable" unless ChaosHelper.docker_available?
    pending! "broker not reachable" unless SpecHelper.broker_reachable?

    Amqp.connect(SpecHelper.amqp_url, recovery: true,
      recovery_initial_delay: 100.milliseconds,
      recovery_max_attempts: 30) do |conn|
      ch = conn.channel
      queue = "amqp-ng-chaos-restart-#{Random::Secure.hex(4)}"
      info = ch.queue_declare(queue, auto_delete: true)
      sub = ch.consume(info.name, no_ack: true)

      ChaosHelper.docker("restart", container)

      deadline = Time.instant + 20.seconds
      loop do
        break if conn.recovered_to_open?
        fail "connection did not recover after broker restart" if Time.instant > deadline
        sleep 100.milliseconds
      end

      ch.publish("", info.name, "after-restart".to_slice)
      delivery = sub.receive
      String.new(delivery.body).should eq("after-restart")
    end
  end
end
