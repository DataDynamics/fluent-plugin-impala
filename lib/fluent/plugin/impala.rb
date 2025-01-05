require 'prometheus/client'
require 'prometheus/client/push'
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
        print "Impala Run Started\n"
        if record[:message].include?("THRIFT_EAGAIN (timed out)")
          @record.store("type", "TIMEOUT")
        end

        if record[:message].include?("Invalid or unknown query handle")
          @record.store("type", "INVALID_HANDLE")
        end

        if record[:message].include?("Exec() query_id=")
          @record.store("type", "QUERY")
          @record.store("query", impala_query(record["message"]))
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