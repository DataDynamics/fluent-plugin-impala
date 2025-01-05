module Fluent
  module Plugin
    class Kudu

      attr_accessor :record

      def initialize(record)
        @record = record
      end

      def run
        print "Kudu Run Started\n"

        if @record["message"].include?("The service queue is full")
          @record.store("type", "BACKPRESSURE")
          @record.store("items", kudu_thread_pool_item(@record["message"]))
          @record
        end

        if @record["message"].include?("exceeded configure scan timeout")
          r = kudu_scan_timeout(@record["message"])
          @record.store("type", "SCAN_TIMEOUT")
          @record.store("current_running_tasks", r["current_running_tasks"])
          @record.store("max_running_tasks", r["max_running_tasks"])
          @record.store("current_queued_tasks", r["current_queued_tasks"])
          @record.store("max_queued_tasks", r["max_queued_tasks"])
          @record.store("items", r["items"])
          @record
        end

        if @record["message"]..include?("THRIFT_EAGAIN (timed out)")
          @record.store("type", "THRIFT_EAGAIN_TIMEOUT")
          @record
        end
      end

      def kudu_backpressure(message)
        result = {}
        result.merge!(kudu_thread_pool_task_running(message))
        result.merge!(kudu_thread_pool_task_queued(message))
        result.merge!(kudu_thread_pool_item(message))
        result
      end

      def kudu_scan_timeout(message)
        result = {}
        start_index = message.index("for Kudu table ‘") + 1
        end_index = message.index("' : Time out : exceeded configure scan timeout") - 1
        table_name = message[start_index..end_index].strip
        result.store("table_name", table_name)
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
        result.store("items", message[start_index..end_index].strip)
        result
      end
    end
  end
end