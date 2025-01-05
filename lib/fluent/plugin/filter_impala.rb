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

      def kudu_backpressure(message)
        result = {}
        matched = message.include?("The service queue is full")
        if matched
          result.merge!(kudu_thread_pool_task_running(message))
          result.merge!(kudu_thread_pool_task_queued(message))
          result.merge!(kudu_thread_pool_item(message))
          result
        end
        result
      end

      def kudu_thread_pool_task_running(message)
        result = {}
        start_index = message.index("(") + 1
        end_index = message.index("tasks running") - 1
        current_tasks, max_tasks = message[start_index..end_index].strip.split("/").map(&:to_i)
        result.store("current_running_tasks", current_tasks)
        result.store("max_running_tasks", max_tasks)
        result
      end

      def kudu_thread_pool_task_queued(message)
        result = {}
        start_index = message.index("tasks running , ") + 1
        end_index = message.index("tasks queued") - 1
        current_tasks, max_tasks = message[start_index..end_index].strip.split("/").map(&:to_i)
        result.store("current_queued_tasks", current_tasks)
        result.store("max_queued_tasks", max_tasks)
        result
      end

      def kudu_thread_pool_item(message)
        result = {}
        start_index = message.index("The service queue is full; it has ") + 1
        end_index = message.index("items") - 1
        result.store("service_queue_count", message[start_index..end_index].strip)
        result
      end
    end
  end
end
