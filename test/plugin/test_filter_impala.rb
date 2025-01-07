require "helper"
require "fluent/plugin/filter_impala.rb"

class ImpalaFilterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  CONFIG = %[
    engine kudu
  ]

  test "include_test" do
    message = { "message": "Row Projector request submit failed: service unavailable: Thread pool is at capacity(1/1 tasks running , 100/100 tasks queued) [suppressed 79 similar messages]Proxy.cc:231] call had error, refreshing address and retrying: Remote error: Service unavailable: Update Consensus request on kudu.consensus. consensusService from 1.1.1.1:55108 dropped due to backpressure. The service queue is full; it has 50 items" }

    print "RESULT : " + message[:message].include?("The service queue is full").to_s + "\n"
  end

  test "failure" do
    messages1 = [{ "message": "I0104 23:42:57.347990 1154049 coordinator.cc:151] af4570722c270ac5:01f64dc100000000] Exec() query_id=af4570722c270ac5:01f64dc100000000 stmt=SELECT `t`.`company`, `t`.`gbm`, `t`.`createdate`, `t`.`datatype`, `t`.`isvalid`, `t`.`sono`, `t`.`soitem`, `t`.`sosche`, `t`.`documentno`, `t`.`documentitem`, `t`.`atpdoc`, `t`.`dmdship`, `t`.`dmdlnitem` FROM `kududb`.`dyn_salesorder_dp` `t` LIMIT 501" }]
    messages2 = [{ "message": "I0104 01:17:53.624439 897431 impala-server.cc:1632] Invalid or unknown query handle: 5944906400e507af:990bc41b00000000." }]
    messages3 = [{ "message": "I0104 23:42:57.324193 1135042 Frontend.java:2000] af4570722c270ac5:01f64dc100000000] Analyzing query: SELECT `t`.`company`, `t`.`gbm`, `t`.`createdate`, `t`.`datatype`, `t`.`isvalid`, `t`.`sono`, `t`.`soitem`, `t`.`sosche`, `t`.`documentno`, `t`.`documentitem`, `t`.`atpdoc`, `t`.`dmdship`, `t`.`dmdlnitem` FROM `kududb`.`dyn_salesorder_dp` `t` LIMIT 501 db: default" }]
    messages4 = [{ "message": "I1014 05:03:12.542078 3712347 thrift-util.cc:196] TSocket::read() THRIFT_EAGAIN (timed out) after %f ms: 알 수 없는 오류30000" }]
    messages5 = [{ "message": "Unable to open scanner for node with id ‘0’ for Kudu table ‘nc.d_so’ : Time out : exceeded configure scan timeout of 180.000s: Scan RPC to x.x.x.x:7050 timed out after 179.999 (ON_OUTBOUND_QUEUE)" }]
    messages6 = [{ "message": "Row Projector request submit failed: service unavailable: Thread pool is at capacity(1/1 tasks running , 100/100 tasks queued) [suppressed 79 similar messages]Proxy.cc:231] call had error, refreshing address and retrying: Remote error: Service unavailable: Update Consensus request on kudu.consensus. consensusService from 1.1.1.1:55108 dropped due to backpressure. The service queue is full; it has 50 items" }]
    messages7 = [{ "message": "Failed to write batch of 6 ops to tablet after 1 attempt : Failed to write to server : Write RPC to  x.x.x.x:7050 timed out after 179.999 (ON_OUTBOUND_QUEUE)" }]

    expected = [
      {
        "message" => "test"
      }
    ]

    filtered_records = filter(CONFIG, messages6)

    print "\n"
    print "\n"
    print filtered_records
    print "\n"
    print "\n"
  end

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
