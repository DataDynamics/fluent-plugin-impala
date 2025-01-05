require "fluent/plugin/filter"

module Fluent
  module Plugin
    class ImpalaFilter < Fluent::Plugin::Filter
      Fluent::Plugin.register_filter("impala", self)

      def filter(tag, time, record)
      end
    end
  end
end
