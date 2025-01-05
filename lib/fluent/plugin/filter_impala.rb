require "fluent/plugin/filter"

module Fluent
  module Plugin
    class ImpalaFilter < Fluent::Plugin::Filter
      Fluent::Plugin.register_filter("impala", self)

      def configure(conf)
        super
      end

      # tag string, time Fluent::EventTime or Integer, record Hash
      def filter(tag, time, record)
        print "FILTER STARTED"
        record
      end
    end
  end
end
