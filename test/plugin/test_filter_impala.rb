require "helper"
require "fluent/plugin/filter_impala.rb"

class ImpalaFilterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  CONFIG = %[
    param1 value1
    param2 value2
  ]

  test "failure" do
    messages = [
      {
        "message" => "test"
      }
    ]

    expected = [
      {
        "message" => "test"
      }
    ]

    filtered_records = filter(CONFIG, messages)

    print filtered_records
  end

  def filter(config, messages)
    print config
    d = create_driver(config)
    d.run(default_tag: "test") do
      messages.each do |message|
        d.feed(message)
      end
    end
  end

  private

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::ImpalaFilter).configure(conf)
  end
end
