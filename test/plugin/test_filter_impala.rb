require "helper"
require "fluent/plugin/filter_impala.rb"

class ImpalaFilterTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  test "failure" do
    flunk
  end

  private

  def create_driver(conf)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::ImpalaFilter).configure(conf)
  end
end
