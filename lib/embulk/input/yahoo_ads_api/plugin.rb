require 'time'
require 'csv'
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
            :servers => config.param("servers", :string),
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
          token = Auth.new({
            client_id: task["client_id"],
            client_secret: task["client_secret"],
            refresh_token: task["refresh_token"] 
          }).get_token
          if task["target"] == "report"
            column_list = Column.send(task[:report_type] != nil ? task[:report_type].downcase : "ydn")
            data = ReportClient.new(task["servers"], task["account_id"], token).run({
              servers: task["servers"],
              date_range_type: 'CUSTOM_DATE',
              start_date: task["start_date"],
              end_date: task["end_date"],
              report_type: task["report_type"],
              fields: task["columns"]
            })
            # delete the total data of specified period
            data.delete(-1)
          elsif task["target"] == "stats"
            column_list = Column.stats
            data = StatsClient.new(task["servers"], task["account_id"], token).run({
              servers: task["servers"],
              date_range_type: 'CUSTOM_DATE',
              start_date: task["start_date"],
              end_date: task["end_date"],
              stats_type: task["report_type"],
            })
          end
          data.each do |row|
            page_builder.add(task["columns"].map do|column|
              col = column_list.find{|c| c[:request_name] == column["name"]}
              if column["type"] == "timestamp"
                Time.strptime(row[col[:api_name]],column["format"])
              elsif column["type"] == "long"
                row[col[:api_name]].to_i
              elsif column["type"] == "double"
                row[col[:api_name]].to_f
              else
                row[col[:api_name]]
              end
            end)
          end
          page_builder.finish

          task_report = {}
          return task_report
        end
      end
    end
  end
end
