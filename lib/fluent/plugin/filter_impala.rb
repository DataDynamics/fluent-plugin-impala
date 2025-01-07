require "fluent/plugin/filter"
require "fluent/plugin/kudu.rb"
require "fluent/plugin/impala.rb"

module Fluent
  module Plugin
    class ImpalaFilter < Fluent::Plugin::Filter
      Fluent::Plugin.register_filter("impala", self)

      attr_accessor :engine

      def configure(conf)
        super
        $log.debug "Impala Plugin Configure Started\n"
        $log.debug "Impala Plugin - Engine: #{conf["engine"]}\n"

        @engine = conf["engine"]
      end

      # tag string, time Fluent::EventTime or Integer, record Hash
      def filter(tag, time, record)
        if @engine == "impala"
          r = Impala.new(record).run
        else
          r = Kudu.new(record).run
        end
      end
    end
  end
end
