require "helper"
require "fluent/plugin/filter_impala.rb"
require "test/unit"

class ImpalaFilterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  CONFIG = %[
    engine kudu
  ]

  messages5 = [{ "message": "Unable to open scanner for node with id ‘0’ for Kudu table ‘nc.d_so’ : Time out : exceeded configure scan timeout of 180.000s: Scan RPC to x.x.x.x:7050 timed out after 179.999 (ON_OUTBOUND_QUEUE)" }]
  messages6 = [{ "message": "Row Projector request submit failed: service unavailable: Thread pool is at capacity(1/1 tasks running , 100/100 tasks queued) [suppressed 79 similar messages]Proxy.cc:231] call had error, refreshing address and retrying: Remote error: Service unavailable: Update Consensus request on kudu.consensus. consensusService from 1.1.1.1:55108 dropped due to backpressure. The service queue is full; it has 50 items" }]
  messages7 = [{ "message": "Failed to write batch of 6 ops to tablet after 1 attempt : Failed to write to server : Write RPC to  x.x.x.x:7050 timed out after 179.999 (ON_OUTBOUND_QUEUE)" }]

  test "messages5_test" do
    assert_true true, messages5[0][:message].include?("exceeded configure scan timeout")

    r = filter(CONFIG, messages5)

    print r

    assert_equal "SCAN_TIMEOUT", r[0]["type"]
    assert_equal "nc.d_so", r[0]["table_name"]
  end

  test "messages6_test" do
    assert_true true, messages6[0][:message].include?("The service queue is full")

    r = filter(CONFIG, messages6)

    print r

    assert_equal "BACKPRESSURE", r[0]["type"]
    assert_equal 1, r[0]["current_running_tasks"]
    assert_equal 1, r[0]["max_running_tasks"]
    assert_equal 100, r[0]["current_queued_tasks"]
    assert_equal 100, r[0]["max_queued_tasks"]
  end

  test "messages7_test" do
    assert_true true, messages7[0][:message].include?("Failed to write batch")

    r = filter(CONFIG, messages7)

    print r

    assert_equal "WRITER_TIMEOUT", r[0]["type"]
  end

  ##########################################################
  # Test Driver
  ##########################################################

  def filter(config, messages)
    driver = create_driver(config)
    driver.run(default_tag: "test") do
      messages.each do |message|
        driver.feed(message) # 로그 메시지를 주입
      end
    end
  end

  private

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::ImpalaFilter).configure(conf)
  end
end
