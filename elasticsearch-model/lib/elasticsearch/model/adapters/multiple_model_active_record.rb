module Elasticsearch
  module Model
    module Adapter

      # An adapter for multiple ActiveRecord-based models
      #
      module MultipleModelActiveRecord

        Adapter.register self,
                         lambda { |klass| klass.is_a? MultipleModels }

        module Records
          # Returns an `ActiveRecord::Relation` instance
          #
          def records
            combined_records = []
            grouped_records = ids_with_klass(ids_with_type)
            grouped_records.each do |_klass, _ids|
              sql_records = _klass.where(_klass.primary_key => _ids)

              # Re-order records based on the order from Elasticsearch hits
              # by redefining `to_a`, unless the user has called `order()`
              #
              sql_records.instance_exec(response.response['hits']['hits']) do |hits|
                define_singleton_method :to_a do
                  if defined?(::ActiveRecord) && ::ActiveRecord::VERSION::MAJOR >= 4
                    self.load
                  else
                    self.__send__(:exec_queries)
                  end
                  @records.sort_by { |record| hits.index { |hit| hit['_id'].to_s == record.id.to_s } }
                end
              end

              combined_records += sql_records
            end
            MultipleModelRecords.new(combined_records)
          end

          # Prevent clash with `ActiveSupport::Dependencies::Loadable`
          #
          def load
            records.load
          end

          # Intercept call to the `order` method, so we can ignore the order from Elasticsearch
          #
          def order(*args)
            sql_records = records.__send__ :order, *args

            # Redefine the `to_a` method to the original one
            #
            sql_records.instance_exec do
              define_singleton_method(:to_a) do
                if defined?(::ActiveRecord) && ::ActiveRecord::VERSION::MAJOR >= 4
                  self.load
                else
                  self.__send__(:exec_queries)
                end
                @records
              end
            end

            sql_records
          end

          # Create an array of hashes for classes and ids
          #
          # @return [Array] Array of hashes [ {:type => [:ids] } ]
          def ids_with_klass(mixed_records)
            grouped = Hash.new
            mixed_records.each do |record|
              k = Kernel.const_get(record[:type].capitalize)
              grouped[k] ||= []
              grouped[k] << record[:id]
            end
            grouped
          end

          class MultipleModelRecords
            def initialize(relation)
              @relation = relation.to_a
            end
            def for(_klass)
              @relation.map do |obj|
                obj if obj.is_a? _klass
              end.compact
            end
          end
        end

        module Callbacks
        end

        module Importing
        end
      end
    end
  end
end
