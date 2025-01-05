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
        print record
        record
      end

      def kudu_backpressure(message)
        result = {}
        matched = message.include?("The service queue is full")
        if matched
          result.merge!(_kudu_thread_pool_task_running(message))
          result.merge!(_kudu_thread_pool_task_queued(message))
          result.merge!(_kudu_thread_pool_item(message))
          result
        end
        result
      end

      def kudu_scan_timeout(message)
        result = {}
        matched = message.include?("exceeded configure scan timeout")
        if matched
          result.merge!(_kudu_scan_timout_table_name(message))
          result
        end
        result
      end

      def impala_thrift_eagain_timeout(message)
        matched = message.include?("THRIFT_EAGAIN (timed out)")
        if matched
          true
        end
        false
      end

      def impala_invalid_handle(message)
        matched = message.include?("Invalid or unknown query handle")
        if matched
          true
        end
        false
      end

      def impala_query(message)
        result = {}
        matched = message.include?("Exec() query_id=")
        if matched
          start_index = message.index("stmt=") + 1
          end_index = message.length - 1
          query = message[start_index..end_index].strip
          result.store("query", query)
        end
        result.store("query", "")
        result
      end

      #############################################################
      # Internal Use Only
      #############################################################

      def _kudu_scan_timout_table_name(message)
        result = {}
        start_index = message.index("for Kudu table ‘") + 1
        end_index = message.index("' : Time out : exceeded configure scan timeout") - 1
        table_name = message[start_index..end_index].strip
        result.store("table_name", table_name)
        result
      end

      def _kudu_thread_pool_task_running(message)
        result = {}
        start_index = message.index("(") + 1
        end_index = message.index("tasks running") - 1
        current_tasks, max_tasks = message[start_index..end_index].strip.split("/").map(&:to_i)
        result.store("current_running_tasks", current_tasks)
        result.store("max_running_tasks", max_tasks)
        result
      end

      def _kudu_thread_pool_task_queued(message)
        result = {}
        start_index = message.index("tasks running , ") + 1
        end_index = message.index("tasks queued") - 1
        current_tasks, max_tasks = message[start_index..end_index].strip.split("/").map(&:to_i)
        result.store("current_queued_tasks", current_tasks)
        result.store("max_queued_tasks", max_tasks)
        result
      end

      def _kudu_thread_pool_item(message)
        result = {}
        start_index = message.index("The service queue is full; it has ") + 1
        end_index = message.index("items") - 1
        result.store("service_queue_count", message[start_index..end_index].strip)
        result
      end
    end
  end
end
