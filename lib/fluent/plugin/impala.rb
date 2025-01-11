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
        if record["message"].include?("stmt=")
          prefix = "collect_impala_query"
          @record.store("category", "prometheus")
          @record.store("type", "QUERY")
          @record.store("query", impala_query(record["message"]))
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "#{prefix}_count")
          @record.store("metric-prefix", prefix)
          @record.store("metric-desc", "Impala Query 건수")
          @record
        end

        if record["message"].include?("Invalid or unknown query handle")
          prefix = "collect_impala_invalid_handle"
          @record.store("category", "prometheus")
          @record.store("type", "SYSTEM")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "#{prefix}_count")
          @record.store("metric-prefix", prefix)
          @record.store("metric-desc", "Invalid Query Handle 건수")
          @record
        end

        if record["message"].include?("THRIFT_EAGAIN (timed out)")
          prefix = "collect_impala_thrift_timeout_count"
          @record.store("category", "prometheus")
          @record.store("type", "TIMEOUT")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "#{prefix}_count")
          @record.store("metric-prefix", prefix)
          @record
          @record.store("metric-desc", "THRIFT EAGAIN Timeout 건수")
        end
      end

      def impala_query(message)
        start_index = message.index("stmt=") + 1
        end_index = message.length - 1
        message[start_index..end_index].strip
      end
    end
  end
end
