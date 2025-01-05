require "fluent/plugin/filter"
require "fluent/plugin/kudu.rb"
require "fluent/plugin/impala.rb"
require 'rufus-scheduler'
require 'prometheus/client'
require 'prometheus/client/push'

module Fluent
  module Plugin
    class ImpalaFilter < Fluent::Plugin::Filter
      Fluent::Plugin.register_filter("impala", self)

      attr_accessor :engine, :scheduler, :registry, :gateway

      def configure(conf)
        super
        print "Configure Started\n"
        print "Configured Engine: #{conf["engine"]}\n"

        @engine = conf["engine"]
        @gateway = conf["gateway"]

        @scheduler = Rufus::Scheduler.new
        @registry = Prometheus::Client.registry

        @scheduler.every '15s' do
          print "Scheduler Runned\n"
        end
      end

      # tag string, time Fluent::EventTime or Integer, record Hash
      def filter(tag, time, record)
        if @engine == "impala"
          r = Impala.new(record).run
          print r
        else
          r = Kudu.new(record).run
          print r
        end
      end
    end
  end
end
