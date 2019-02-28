module Sidekiq
  module DynamicQueues

    DYNAMIC_QUEUE_KEY = "dynamic_queue"
    FALLBACK_KEY = "default"

    module Attributes
      extend self

      def json_encode(data)
        Sidekiq.dump_json(data)
      end

      def json_decode(data)
        return nil unless data
        Sidekiq.load_json(data)
      end

      def get_dynamic_queue(key, fallback=['*'])
        data = Sidekiq.redis {|r| r.hget(DYNAMIC_QUEUE_KEY, key) }
        queue_names = json_decode(data)

        if queue_names.nil? || queue_names.size == 0
          data = Sidekiq.redis {|r| r.hget(DYNAMIC_QUEUE_KEY, FALLBACK_KEY) }
          queue_names = json_decode(data)
        end

        if queue_names.nil? || queue_names.size == 0
          queue_names = fallback
        end

        return queue_names
      end

      def set_dynamic_queue(key, values)
        if values.nil? or values.size == 0
          Sidekiq.redis {|r| r.hdel(DYNAMIC_QUEUE_KEY, key) }
        else
          Sidekiq.redis {|r| r.hset(DYNAMIC_QUEUE_KEY, key, json_encode(values)) }
        end
      end

      def set_dynamic_queues(dynamic_queues)
        Sidekiq.redis do |r|
          r.multi do
            r.del(DYNAMIC_QUEUE_KEY)
            dynamic_queues.each do |k, v|
              set_dynamic_queue(k, v)
            end
          end
        end
      end

      def get_dynamic_queues
        result = {}
        queues = Sidekiq.redis {|r| r.hgetall(DYNAMIC_QUEUE_KEY) }
        queues.each {|k, v| result[k] = json_decode(v) }
        result[FALLBACK_KEY] ||= ['*']
        return result
      end

      # Returns a list of queues to use when searching for a job.
      #
      # A splat ("*") means you want every queue (in alpha order) - this
      # can be useful for dynamically adding new queues.
      #
      # The splat can also be used as a wildcard within a queue name,
      # e.g. "*high*", and negation can be indicated with a prefix of "!"
      #
      # An @key can be used to dynamically look up the queue list for key from redis.
      # If no key is supplied, it defaults to the worker's hostname, and wildcards
      # and negations can be used inside this dynamic queue list.   Set the queue
      # list for a key with
      # Sidekiq::DynamicQueues::Attributes.set_dynamic_queue(key, ["q1", "q2"]
      #
      def expand_queues(queues, real_queues = nil)
        real_queues ||= Sidekiq::Queue.all.map(&:name)
        expansions = queues.map(&:to_s).uniq.map do |q|
          [q, expand_queue(q, real_queues).uniq]
        end.to_h

        # negated queues only remove the previously matched results
        negations = []
        expansions.reverse_each do |q, matches|
          negations.concat matches if q =~ /^!/
          expansions[q] -= negations
        end
        expansions.reject! { |_q, matches| matches.empty? }

        # preserve weights using the least common multiple to expand all dynamic queues to the same size
        lcm = expansions.values.map(&:size).reduce(1, :lcm)
        weighted_queues = expansions.map do |q, matches|
          weight = (q[/\.(\d+)$/, 1] || '1').to_i * queues.count(q) * lcm / matches.size
          matches.zip([weight] * matches.size).to_h
        end
        weighted_queues.each_with_object({}) do |hash, result|
          hash.each do |queue, weight|
            result[queue] ||= 0
            result[queue] += weight
          end
        end
      end

      def expand_queue(q, real_queues)
        q = q[1..-1] if q =~ /^!/

        if q =~ /^@(.*)/
          key = q[1..-1].strip
          key = hostname if key.empty?

          # TODO: preserve weight for dynamic queues
          expand_queues(get_dynamic_queue(key), real_queues).keys
        elsif q =~ /\*/
          patstr = q.gsub(/\*/, '.*').sub(/\.\d+$/, '')
          real_queues.grep(/^#{patstr}$/)
        else
          [q.sub(/\.\d+$/, '')]
        end
      end
    end
  end
end
