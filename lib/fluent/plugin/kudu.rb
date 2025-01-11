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

        # W0827 10:27:11.563266 18970 leader_election.cc:287] T 5e52666b9ecf4460808b387a7bfe67d6 P e8a1d4cacb7f4f219fd250704004d258 [CANDIDATE]: Term 1968 pre-election: RPC error from VoteRequest() call to peer 3a5ee46ab1284f4e9d4cdfe5d0b7f7fa: Remote error: Service unavailable: RequestConsensusVote request on kudu.consensus.ConsensusService from 10.250.250.19:37042 dropped due to backpressure. The service queue is full; it has 50 items.

        if record["message"].include?("The service queue is full")
          @record.store("category", "prometheus")
          @record.store("type", "BACKPRESSURE")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "collect_kudu_backpressure_count")
          @record.store("metric-desc", "Kudu Backpressure 건수")
          @record.store("items", kudu_thread_pool_item(record["message"])["items"])
          r = kudu_backpressure(record["message"])
          @record.store("current_running_tasks", r["current_running_tasks"])
          @record.store("max_running_tasks", r["max_running_tasks"])
          @record.store("current_queued_tasks", r["current_queued_tasks"])
          @record.store("max_queued_tasks", r["max_queued_tasks"])
          @record
        end

        if record["message"].include?("exceeded configure scan timeout")
          @record.store("category", "prometheus")
          @record.store("type", "SCAN_TIMEOUT")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "collect_kudu_scan_timeout_count")
          @record.store("metric-desc", "Kudu Scan Timeout 건수")
          start_index = record["message"].index("for Kudu table ‘") + "for Kudu table ‘".length
          end_index = record["message"].index("’ : Time out") - 1
          @record.store("table_name", record["message"][start_index..end_index].strip)
          @record
        end

        # Timed out: Failed to write batch of 227 ops to tablet 8b19e4a0362e4b82941e54d33ac9c5a2 after 1 attempt(s): Failed to write to server: b2ead65ab0164f5b8db24d700a2c474a (wewcw0hd3dn02.example.com:7050): Write RPC to 10.11.100.85:7050 timed out after 179.977s (SENT)

        if record["message"].include?("Failed to write batch")
          @record.store("category", "prometheus")
          @record.store("type", "WRITER_TIMEOUT")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "collect_kudu_write_batch_timeout_count")
          @record.store("metric-desc", "Kudu Failed to Write Batch 건수")
          @record
        end

        # W0922 00:56:52.313848 10858 inbound_call.cc:193] Call kudu.consensus.ConsensusService.UpdateConsensus
        # from 192.168.1.102:43499 (request call id 3555909) took 1464ms (client timeout 1000).

        if record["message"].include?("client timeout")
          @record.store("category", "prometheus")
          @record.store("type", "CLIENT_TIMEOUT")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "collect_kudu_client_timeout_count")
          @record.store("metric-desc", "Kudu Client Timeout 건수")
          @record
        end

        # Service unavailable: Soft memory limit exceeded (at 96.35% of capacity)

        if record["message"].include?("Soft memory limit exceeded")
          @record.store("category", "prometheus")
          @record.store("type", "MEMORY_LIMIT_EXCEEDED")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "collect_kudu_soft_memory_limit_exceeded_count")
          @record
        end

        # W0926 11:19:01.339553 27231 net_util.cc:129] Time spent resolving address for kudu-tserver.example.com: real 4.647s    user 0.000s     sys 0.000s

        if record["message"].include?("Time spent resolving address")
          @record.store("category", "prometheus")
          @record.store("type", "DNS_RESOLVE_SLOW")
          @record.store("metric-type", "Counter")
          @record.store("metric-name", "kudu_dns_resolve_slow_count")
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