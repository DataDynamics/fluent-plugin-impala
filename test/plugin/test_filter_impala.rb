require "helper"
require "fluent/plugin/filter_impala.rb"
require "test/unit"

class ImpalaFilterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  CONFIG = %[
    engine impala
  ]

  messages1 = [{ "message": "I0104 23:42:57.347990 1154049 coordinator.cc:151] af4570722c270ac5:01f64dc100000000] Exec() query_id=af4570722c270ac5:01f64dc100000000 stmt=SELECT `t`.`company`, `t`.`gbm`, `t`.`createdate`, `t`.`datatype`, `t`.`isvalid`, `t`.`sono`, `t`.`soitem`, `t`.`sosche`, `t`.`documentno`, `t`.`documentitem`, `t`.`atpdoc`, `t`.`dmdship`, `t`.`dmdlnitem` FROM `kududb`.`dyn_salesorder_dp` `t` LIMIT 501" }]
  messages2 = [{ "message": "I0104 01:17:53.624439 897431 impala-server.cc:1632] Invalid or unknown query handle: 5944906400e507af:990bc41b00000000." }]
  messages3 = [{ "message": "I1014 05:03:12.542078 3712347 thrift-util.cc:196] TSocket::read() THRIFT_EAGAIN (timed out) after %f ms: 알 수 없는 오류30000" }]

  test "message1_test" do
    assert_true true, messages1[0][:message].include?("stmt=")

    r = filter(CONFIG, messages1)

    print r

    assert_equal "QUERY", r[0]["type"]
  end

  test "message2_test" do
    assert_true true, messages2[0][:message].include?("Invalid or unknown query handle")

    r = filter(CONFIG, messages2)

    print r

    assert_equal "INVALID_HANDLE", r[0]["type"]
  end

  test "message3_test" do
    assert_true true, messages3[0][:message].include?("THRIFT_EAGAIN (timed out)")

    r = filter(CONFIG, messages3)

    print r

    assert_equal "THRIFT_TIMEOUT", r[0]["type"]
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
