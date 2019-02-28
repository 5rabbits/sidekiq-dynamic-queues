require 'sidekiq/fetch'

module Sidekiq
  module DynamicQueues

    # enable with:
    #    Sidekiq.configure_server do |config|
    #        config.options[:fetch] = Sidekiq::DynamicQueues::Fetch
    #    end
    #
    class Fetch < Sidekiq::BasicFetch

      include Sidekiq::Util
      include Sidekiq::DynamicQueues::Attributes

      def initialize(options)
        super
        @dynamic_queues = self.class.translate_from_cli(*options[:queues])
      end

      # overriding Sidekiq::BasicFetch#queues_cmd
      def queues_cmd
        if @dynamic_queues.grep(/(^!)|(^@)|(\*)/).size == 0
          super
        else
          expanded = expand_queues(@dynamic_queues)
          queues = if @strictly_ordered_queues
                     expanded.keys.uniq
                   else
                     weighted_key_shuffle(expanded)
                   end
          queues << 'default' if queues.empty?
          queues = queues.uniq.map { |q| "queue:#{q}" }
          queues << TIMEOUT
        end
      end

      def weighted_key_shuffle(queues_hash)
        queues = []
        while queues_hash.present?
          queue = random_key(queues_hash)
          queues << queue
          queues_hash = queues_hash.except(queue)
        end
        queues
      end

      def random_key(queues_hash)
        r = rand(queues_hash.values.sum)
        sum = 0
        queues_hash.each do |queue, priority|
          sum += priority
          return queue if r < sum
        end
        # this would only happen if all priorities are 0
        queues_hash.keys.first
      end

      def self.translate_from_cli(*queues)
        queues.collect do |queue|
          queue.gsub('.star.', '*').gsub('.at.', '@').gsub('.not.', '!')
        end
      end

    end

  end
end
