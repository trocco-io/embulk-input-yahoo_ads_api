require 'time'

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
          case task["target"]
          when "report"
            client = ReportClient.new(task["servers"], task["account_id"], token)
            data = client.run({
              servers: task["servers"],
              date_range_type: 'CUSTOM_DATE',
              start_date: task["start_date"],
              end_date: task["end_date"],
              report_type: task["report_type"],
              fields: task["columns"]
            })
          when "stats"
            client = StatsClient.new(task["servers"], task["account_id"], token)
            data = client.run({
              servers: task["servers"],
              date_range_type: 'CUSTOM_DATE',
              start_date: task["start_date"],
              end_date: task["end_date"],
              stats_type: task["report_type"],
            })
          end
          task_column_names = task["columns"].map{|c| c["name"]}
          columns_list = client.columns(task["report_type"]).map { |c|
            [c[:request_name].to_sym, c[:api_name]] if task_column_names.include? c[:request_name]
          }.compact.to_h
          casts = task["columns"].group_by { |c| c["type"] }.map do |k, v|
            case k
            when "timestamp"
              values = v.map { |c| [task["columns"].index { |sc| sc == c }, c["format"]] }.compact
              [k, values]
            when "long", "double"
              values = v.map { |c| task["columns"].index { |sc| sc == c } }.compact
              [k, values]
            else
              nil
            end
          end.compact.to_h
          data.each_slice(100) do |rows|
            rows.each do |row|
              row = [].push *row # convert java ArrayList into ruby Array
              casts.each do |k, v|
                case k
                when "timestamp"
                  v.each { |i| row[i[0]] = Time.strptime(row[i[0]], i[1]) }
                when "long"
                  v.each { |i| row[i] = row[i].to_i }
                when "double"
                  v.each { |i| row[i] = row[i].to_f }
                end
              end
              page_builder.add row
            end
          end
          page_builder.finish

          task_report = {}
          return task_report
        end
      end
    end
  end
end
