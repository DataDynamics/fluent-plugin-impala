require 'socket'

module Fluent
  module Plugin
    class Kudu

      attr_accessor :record, :hostname

      def initialize(record)
        @record = record
        @hostname = Socket.gethostname
      end

      def run
        if record["message"].include?("The service queue is full")
          @record.store("category", "prometheus")
          @record.store("type", "BACKPRESSURE")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "kudu_backpressure_count")
          @record.store("items", kudu_thread_pool_item(record["message"])["items"])
          r = kudu_backpressure(record["message"])
          @record.store("current_running_tasks", r["current_running_tasks"])
          @record.store("max_running_tasks", r["max_running_tasks"])
          @record.store("current_queued_tasks", r["current_queued_tasks"])
          @record.store("max_queued_tasks", r["max_queued_tasks"])
        end

        if record["message"].include?("exceeded configure scan timeout")
          @record.store("category", "prometheus")
          @record.store("type", "SCAN_TIMEOUT")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "kudu_scan_timeout_count")
          start_index = record["message"].index("for Kudu table ‘") + "for Kudu table ‘".length
          end_index = record["message"].index("’ : Time out") - 1
          @record.store("table_name", record["message"][start_index..end_index].strip)
        end

        if record["message"].include?("Failed to write batch")
          @record.store("category", "prometheus")
          @record.store("type", "WRITER_TIMEOUT")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "kudu_write_batch_timeout_count")
        end

        @record.store("job", "fluentd-plugin-kudu")
        @record.store("instance", @hostname)
        @record
      end

      def kudu_backpressure(message)
        result = {}
        result.merge!(kudu_thread_pool_task_running(message))
        result.merge!(kudu_thread_pool_task_queued(message))
        result.merge!(kudu_thread_pool_item(message))
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
        start_index = message.index("tasks running , ") + "tasks running , ".length
        end_index = message.index("tasks queued") - 1
        current_tasks, max_tasks = message[start_index..end_index].strip.split("/").map(&:to_i)
        result.store("current_queued_tasks", current_tasks)
        result.store("max_queued_tasks", max_tasks)
        result
      end

      def kudu_thread_pool_item(message)
        result = {}
        pattern = "The service queue is full; it has "
        start_index = message.index(pattern) + pattern.length
        end_index = message.index("items") - 1
        result.store("items", message[start_index..end_index].strip)
        result
      end
    end
  end
end