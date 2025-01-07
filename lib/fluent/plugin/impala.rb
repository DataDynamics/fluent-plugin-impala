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
        if record[:message].include?("stmt=")
          @record.store("category", "prometheus")
          @record.store("type", "QUERY")
          @record.store("query", impala_query(record["message"]))
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "impala_query_count")
        end

        if record[:message].include?("Invalid or unknown query handle")
          @record.store("category", "prometheus")
          @record.store("type", "INVALID_HANDLE")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "impala_invalid_handle_count")
        end

        if record[:message].include?("THRIFT_EAGAIN (timed out)")
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

# test "message1_test" do
#   assert_true true, messages1[0][:message].include?("stmt=")
#
#   r = filter(CONFIG, messages1)
#
#   assert_equal "QUERY", r[0]["type"]
#   assert_equal "SELECT `t`.`company`, `t`.`gbm`, `t`.`createdate`, `t`.`datatype`, `t`.`isvalid`, `t`.`sono`, `t`.`soitem`, `t`.`sosche`, `t`.`documentno`, `t`.`documentitem`, `t`.`atpdoc`, `t`.`dmdship`, `t`.`dmdlnitem` FROM `kududb`.`dyn_salesorder_dp` `t` LIMIT 501", r[0]["table_name"]
# end
#
# test "message2_test" do
#   assert_true true, messages2[0][:message].include?("Invalid or unknown query handle")
#
#   r = filter(CONFIG, messages2)
#
#   assert_equal "INVALID_HANDLE", r[0]["type"]
#   assert_equal "5944906400e507af:990bc41b00000000", r[0]["query_id"]
# end
#
# test "message3_test" do
#   assert_true true, messages3[0][:message].include?("THRIFT_EAGAIN (timed out)")
#
#   r = filter(CONFIG, messages3)
#
#   assert_equal "THRIFT_TIMEOUT", r[0]["type"]
# end
