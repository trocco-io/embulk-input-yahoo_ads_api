require "time"

module Embulk
  module Input
    module YahooAdsApi
      class Plugin < InputPlugin
        ::Embulk::Plugin.register_input("yahoo_ads_api", self)

        def self.transaction(config, &control)
          # configuration code:
          task = {
            :target => config.param("target", :string, default: "report"),
            :client_id => config.param("client_id", :string),
            :client_secret => config.param("client_secret", :string),
            :refresh_token => config.param("refresh_token", :string),
            :ads_type => config.param("ads_type", :string),
            :columns => config.param("columns", :array),
            :account_id => config.param("account_id", :string),
            :report_type => config.param("report_type", :string, default: nil),
            :start_date => config.param("start_date", :string),
            :end_date => config.param("end_date", :string),
          }
          columns = task[:columns].map do |column|
            ::Embulk::Column.new(nil, column["name"], column["type"].to_sym)
          end
          resume(task, columns, 1, &control)
        end

        def self.resume(task, columns, count, &control)
          task_reports = yield(task, columns, count)
          next_config_diff = {}
          return next_config_diff
        end

        def init
        end

        def run
          ConfigCheck.check(task)
          DataProcessor.new(task).run { |row| page_builder.add row }
          page_builder.finish
          task_report = {}
          return task_report
        end
      end
    end
  end
end
