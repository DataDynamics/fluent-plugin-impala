require 'socket'

module Fluent
  module Plugin
    class Impala

      attr_accessor :record, :hostname

      def initialize(record)
        @record = record
        @hostname = Socket.gethostname
      end

      def run

        $log.debug "==================================\n\n"
        $log.debug @record["message"]
        $log.debug "==================================\n\n"
        $log.debug @record["message"].to_s
        $log.debug "==================================\n\n"

        if record["message"].include?("stmt=")
          @record.store("category", "prometheus")
          @record.store("type", "QUERY")
          @record.store("query", impala_query(record["message"]))
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "impala_query_count")
        end

        if record["message"].include?("Invalid or unknown query handle")
          @record.store("category", "prometheus")
          @record.store("type", "INVALID_HANDLE")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "impala_invalid_handle_count")
        end

        if record["message"].include?("THRIFT_EAGAIN (timed out)")
          @record.store("category", "prometheus")
          @record.store("type", "THRIFT_TIMEOUT")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "impala_thrift_timeout_count")
        end

        @record.store("job", "fluentd-plugin-impala")
        @record.store("instance", @hostname)
        @record
      end

      def impala_query(message)
        start_index = message.index("stmt=") + 1
        end_index = message.length - 1
        message[start_index..end_index].strip
      end
    end
  end
end
